'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';
import DataSet from '../DataSet';
import superagent from 'superagent';




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


        this.enableAction('create');
        this.enableAction('list');
    }





    /**
    * get the data from data source
    */
    loadData() {
        superagent.get(`${this.dataSet.dataSourceHost}/${this.dataSet.dataSource}.data`).ok(res => res.status === 200).query({
            shard: this.dataSet.shardIdentifier,
            offset: this.dataSet.offset,
            limit: this.pageSize,
        }).send().then((response) => {
            const rows = response.body;

            // add to in memory storage
            this.dataSet.addValues(rows);


            // check how to continue
            if (rows.length === this.pageSize) {

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
    * register a new service
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.dataSource)) response.status(400).send(`Missing parameter or invalid 'dataSource' in request body!`);
        else if (!type.string(data.shardIdentifier)) response.status(400).send(`Missing parameter or invalid 'shardIdentifier' in request body!`);
        else if (!type.number(data.minFreeMemory)) response.status(400).send(`Missing parameter or invalid 'minFreeMemory' in request body!`);
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
            } else response.status(409).send(`Cannot create new data set since the existing data set has not ended (status '${this.dataSet.getCurrentStatusName()}')`);
        }
    }







    /**
    * load all source code from the data source
    */
    async loadSourceCode() {
        const sourceCodeReponse = await superagent.get(`${this.dataSet.dataSourceHost}/${this.dataSet.dataSource}.source-code`).ok(res => res.status === 200).send();
        const map = new Map();

        sourceCodeReponse.body.forEach((sourceItem) => {
            if (!map.has(sourceCode.identifier)) map.set(sourceCode.identifier, {});
            map.get(sourceCode.identifier)[sourceItem.type] = sourceItem.sourceCode;
        });

        this.configuration.set('sourceCode', map);
    }






    /**
    * since initializing the dataset is asynchronous
    * the cluster controller needs to be able to poll 
    * its status. This endpoint is used to do this.
    * the status is loaded from the dataSet instance
    * initialized in the create method.
    */
    async list(request, response) {
        switch(this.dataSet.getCurrentStatusName()) {
            case 'created':
            case 'initializing':
            case 'loading':
            case 'ready':
                return {
                    status: this.dataSet.getCurrentStatusName(),
                };

            case 'loaded':
                response.status(201).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;

            case 'discarded':
                response.status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;

            case 'failed':
                response.status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                    err: this.dataSet.err,
                });
                break;

            default:
                response.status(500).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;
        }
    }
}