import type from 'ee-types';
import logd from 'logd';
import v8 from 'v8';
import SourceCodeManager from './SourceCodeManager.js';
import HTTP2Client from '@distributed-systems/http2-client';


const log = logd.module('rda-compute-dataset');




const statusMap = new Map([
    ['created', 0],
    ['initializing', 100],
    ['ready', 150],
    ['loading', 200],
    ['loaded', 300],
    ['discarded', 1000],
    ['failed', 10000],
]);




export default class DataSet {


    constructor({
        shardIdentifier,
        dataSource,
        minFreeMemory = 25,
        registryClient,
        dataSetIdentifier,
    }) {
        this.dataSetIdentifier = dataSetIdentifier;
        this.shardIdentifier = shardIdentifier;
        this.dataSource = dataSource;
        this.registryClient = registryClient;

        // the amount of memory that must be available
        // in order to assume the process is healthy and
        // can still make use of the data it has.
        // value is percent of the heap size limit
        this.minFreeMemoryPercent = minFreeMemory;

        // there is a pretty rigid status management in order
        // to keep track of things
        this.currentStatus = statusMap.get('created');

        // the offset when loading data
        this.offset = 0;

        // the number of records that should be loaded
        // per page
        this.pageSize = 10000;

        // http 2 client
        this.httpClient = new HTTP2Client();


        this.mappers = new Map();
        this.reducers = new Map();
    }







    /**
    * checks if the memory usage is within the allowed
    * bounds
    */
    isMemoryOk() {
        const usage = v8.getHeapStatistics();
        const currentMemoryUsage = usage.total_available_size/usage.heap_size_limit*100;
        
        log.debug(`[${this.shardIdentifier}] Current memory usage is ${(100-currentMemoryUsage).toFixed(2)}`);

        return currentMemoryUsage > this.minFreeMemoryPercent;
    }



    /**
    * returns true if the dataset has ended
    */
    hasEnded() {
        return this.currentStatus >= statusMap.get('discarded');
    }




    /**
    * prepare the set for accepting data
    */
    async initialize() {
        log.debug(`Initializing data set ${this.shardIdentifier}`);
        this.setStatus('initializing');
        this.dataSourceHost = await this.registryClient.resolve(this.dataSource);
        this.setStatus('ready');
    }




    async load() {

        // change status to loading
        this.prepareForData();

        // initialize the source code management
        this.sourceCodeManager = new SourceCodeManager();

        // get source from storage service
        await this.sourceCodeManager.load(this.dataSourceHost, this.dataSource, this.dataSetIdentifier);

        if (!this.sourceCodeManager.hasModule('InfectModel.js')) {
            throw new Error(`Missing source file 'InfectModel.js'!`);
        }

        // set up the constructor for the model that holds the data
        this.ModelConstructor = await this.sourceCodeManager.runModule('InfectModel.js');

        // start loading data, don't wait for this to finish, since it may take
        // a pretty long time
        this.loadData().then(() => {
            this.complete();
        }).catch((err) => {
            this.fail(err);
        });
    }





    async runReducer(functionName, dataSets) {
        if (!this.reducers.has(functionName)) {
            const Reducer = await this.importSourceFile(`${functionName}Reducer.js`);
            const reducer = new Reducer();

            this.reducers.set(functionName, reducer);
        }

        const reducer = this.reducers.get(functionName);
        return await reducer.compute(dataSets);
    }




    async runMapper(functionName, filterConfiguration) {
        if (!this.mappers.has(functionName)) {
            const Mapper = await this.importSourceFile(`${functionName}Mapper.js`);
            const mapper = new Mapper();

            await mapper.load();
            this.mappers.set(functionName, mapper);
        }

        const mapper = this.mappers.get(functionName);
        const models = this.getValues();

        return await mapper.compute({ models, filterConfiguration });
    }





    async importSourceFile(specifier) {
        if (!this.sourceCodeManager.hasModule(specifier)) {
            throw new Error(`Missing source file '${specifier}'!`);
        }

        return await this.sourceCodeManager.runModule(specifier);
    }




    async loadData() {
        log.debug(`Loading RDA data for shard ${this.shardIdentifier}, offset ${this.offset}, limit ${this.pageSize}`);

        const response = await this.httpClient.get(`${this.dataSourceHost}/${this.dataSource}.data`)
            .expect(200)
            .query({
                shard: this.shardIdentifier,
                offset: this.offset,
                limit: this.pageSize,
            })
            .send();

        // wait for data
        const rows = await response.getData();
        const Model = this.ModelConstructor;

        // add to in memory storage
        this.addValues(rows.map(row => new Model(row)));


        // check how to continue
        if (rows.length === this.pageSize) {

            // next page please!
            this.offset += this.pageSize;
            
            // get another page
            await this.loadData();
        }
    }




    /**
    * set up the data set, make it for accepting values
    */
    prepareForData() {
        log.debug(`Preparing data set ${this.shardIdentifier} for data`);
        this.setStatus('loading');

        // the values of the dataset
        this.values = [];
    }





    /**
    * tell the set that is has all records and thus is 
    * complete
    */
    complete() {
        this.setStatus('loaded');
    }




    /**
    * kill the set, remove all data
    */
    fail(err) {
        this.err = err;
        this.discard();
        this.setStatus('failed');
    }




    /**
    * discard the data set
    */
    discard() {
        this.setStatus('discarded');
        this.values = null;
    }




    /**
    * add new values to the data set
    */
    addValues(values) {
        if (this.currentStatus === statusMap.get('loading')) {
            this.values.push(...values);

            log.debug(`[${this.shardIdentifier}] Value count is ${this.values.length}`);

            // make sure we're not using too much memory
            if (!this.isMemoryOk()) {
                this.fail();
                return false;
            } else return true;
        } else throw new Error(`Cannot add values for data set with the status '${this.getCurrentStatusName()}'! The status 'loading' would be valid.`);
    }


    /**
    * returns the number of records loaded
    */
    getRecordCount() {
        return this.values && this.values.length || 0;
    }


    /**
    * returns the values if the status of the 
    * dataset id viable
    */
    getValues() {
        if (this.currentStatus === statusMap.get('loaded')) {
            return this.values;
        } else throw new Error(`Cannot return values for data set with the status '${this.getCurrentStatusName()}'! The status 'loaded' would be valid.`);
    }




    /**
    * set a new status for this dataset. Only a status with
    * a higher value can be set
    */
    setStatus(status) {
        if (!statusMap.has(status)) throw new Error(`Cannot set invalid status ${status}. Valid status are ${[...statusMap.keys()].join(', ')}.`);
        
        const newStatus = statusMap.get(status);

        if (newStatus > this.currentStatus) {
            this.currentStatus = newStatus;
            log.info(`[${this.shardIdentifier}] Status changed to ${status}`);
        }
        else throw new Error(`Cannopt set status '${status}', it has a lower value (${newStatus}) than the currentStatus '${this.getCurrentStatusName()}' (${this.currentStatus})!`);
    }



    /**
    * return the name for the current status
    */
    getCurrentStatusName() {
        return [...statusMap.entries()].find(item => item[1] === this.currentStatus)[0];
    }
}