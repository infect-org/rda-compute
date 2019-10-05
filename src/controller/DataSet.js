import {Controller} from '@infect/rda-service';
import type from 'ee-types';
import log from 'ee-log';
import DataSet from '../DataSet.js';
import Module from '../Module.js';
import HTTP2Client from '@distributed-systems/http2-client';




export default class DataSetController extends Controller {


    constructor({
        configuration,
        registryClient,
    }) {
        super('data-set');

        this.registryClient = registryClient;

        // service wide configurations received by the cluster 
        // service
        this.configuration = configuration;

        // there can only be one data set, it
        // may be replaced if the status allows
        // it, but there may never be two of them
        this.dataSet = new DataSet({
            shardIdentifier: 'dummy'
        });

        // mark it as discarded so that it can be 
        // replaced and the code managing that has
        // to handle one exception less
        this.dataSet.discard();


        // the number of records that should be loaded
        // per page
        this.pageSize = 10000;


        // http 2 client
        this.httpClient = new HTTP2Client();


        this.enableAction('create');
        this.enableAction('list');
    }



    /**
     * shut down the class
     */
    async end() {
        await this.httpClient.end();
    }



    /**
    * get the data from data source
    */
    loadData() {
        this.httpClient.get(`${this.dataSet.dataSourceHost}/${this.dataSet.dataSource}.data`)
            .expect(200)
            .query({
                shard: this.dataSet.shardIdentifier,
                offset: this.dataSet.offset,
                limit: this.pageSize,
            })
            .send()
            .then(async(response) => {
                const rows = await response.getData();

                // add to in memory storage
                this.dataSet.addValues(rows);


                // check how to continue
                if (rows.length === this.pageSize) {

                    // next page please!
                    this.dataSet.offset += this.pageSize;
                    
                    // get another page
                    this.loadData();
                } else {

                    // the data was loaded, mark as ready
                    this.dataSet.complete();
                }
            }).catch((err) => {
                this.dataSet.fail(err);
            });
    }







    /**
    * laod a data set
    */
    async create(request) {
        const data = await request.getData();

        if (!data) request.response().status(400).send(`Missing request body!`);
        else if (!type.object(data)) request.response().status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.dataSource)) request.response().status(400).send(`Missing parameter or invalid 'dataSource' in request body!`);
        else if (!type.string(data.shardIdentifier)) request.response().status(400).send(`Missing parameter or invalid 'shardIdentifier' in request body!`);
        else if (!type.number(data.minFreeMemory)) request.response().status(400).send(`Missing parameter or invalid 'minFreeMemory' in request body!`);
        else {

            // check if a new dataSet can be created
            // it must have failed or discarded actively
            if (this.dataSet.hasEnded()) {

                // set up the new dataset
                this.dataSet = new DataSet({
                    dataSource: data.dataSource,
                    shardIdentifier: data.shardIdentifier,
                    minFreeMemory: data.minFreeMemory,
                    registryClient: this.registryClient,
                });

                // set the data source service
                this.configuration.set('dataSet', this.dataSet);


                // resolve the data source url
                await this.dataSet.initialize();

                // prepare for new data
                this.dataSet.prepareForData();


                // load the required source code
                await this.loadSourceCode();


                // initialize the 
                this.loadData();
            } else request.response().status(409).send(`Cannot create new data set since the existing data set has not ended (status '${this.dataSet.getCurrentStatusName()}')`);
        }
    }







    /**
    * load all source code from the data source
    */
    async loadSourceCode() {
        const sourceCodeReponse = await this.httpClient.get(`${this.dataSet.dataSourceHost}/${this.dataSet.dataSource}.source-code`)
            .expect(200)
            .send();

        const data = await sourceCodeReponse.getData();
        const map = new Map();

        for (const sourceItem of data) {
            if (!map.has(sourceItem.identifier)) map.set(sourceItem.identifier, {});
            let module;
            
            try {
                module = new Module({
                    sourceCode: sourceItem.sourceCode
                });
            } catch (err) {
                err.message = `Failed to load Script Source Module '${sourceItem.identifier}' from data source '${this.dataSet.dataSource}': ${err.message}`;
                err.sourceCode = sourceItem.sourceCode;
                throw err;
            }

            await module.load();

            map.get(sourceItem.identifier)[sourceItem.type] = module;
        }

        this.configuration.set('sourceCode', map);
    }






    /**
    * since initializing the dataset is asynchronous
    * the cluster controller needs to be able to poll 
    * its status. This endpoint is used to do this.
    * the status is loaded from the dataSet instance
    * initialized in the create method.
    */
    async list(request) {
        switch(this.dataSet.getCurrentStatusName()) {
            case 'created':
            case 'initializing':
            case 'loading':
            case 'ready':
                return {
                    status: this.dataSet.getCurrentStatusName(),
                    recordCount: this.dataSet.getRecordCount(),
                };

            case 'loaded':
                request.response().status(201).send({
                    status: this.dataSet.getCurrentStatusName(),
                    recordCount: this.dataSet.getRecordCount(),
                });
                break;

            case 'discarded':
                request.response().status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                    recordCount: this.dataSet.getRecordCount(),
                });
                break;

            case 'failed':
                request.response().status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                    recordCount: this.dataSet.getRecordCount(),
                    err: this.dataSet.err,
                });
                break;

            default:
                request.response().status(500).send({
                    status: this.dataSet.getCurrentStatusName(),
                    recordCount: this.dataSet.getRecordCount(),
                });
                break;
        }
    }
}