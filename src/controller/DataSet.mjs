'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';
import DataSet from '../DataSet';




export default class DataSetController extends Controller {


    constructor({
        configuration
    }) {
        super('data-set');

        // service wide configurations received by the cluster 
        // service
        this.configuration = configuration;

        // there can only be one data set, it
        // may be replaced if the status allows
        // it, but there may never be two of them
        this.dataSet = new DataSet();

        // mark it as discarded so that it can be 
        // replaced and the code managing that has
        // to handle one exception less
        this.dataSet.discard();


        this.enableAction('create');
    }






    /**
    * initialize the data set, instruct the data
    * source to prepare its shards. load the data 
    * from the source service and manage the status
    * of this service so that asynchronous http calls
    * work properly and get a valid response
    */
    initialize() {
        Promise.resolve().then(async () => {

            // tell the data source to prepare the sahrds
            await this.requestDataPreparation();

            // load data
        }).catch((err) => {

            // this sucks :/
            this.dataSet.fail(err);
        });
    }








    /**
    * tell the source service that it needs to prepare 
    * the data shards so that they can be consumed by 
    * this service.
    */
    async requestDataPreparation() {
        this.dataSet.initialize();


        // call the data source, ask it to create 
        // the required shards
        
    }






    /**
    * register a new service
    */
    async create(request, response) {
        const data = request.body;

        // make sure the name of this service was
        // assigned before it can load the data 
        // from the remote server
        if (!this.configuration.has('name')) throw new Error(`The service was not configured an cannot laod data. Please call the configuration controller first and set the 'name' parameter! `);


        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.sourceService)) response.status(400).send(`Missing parameter or invalid 'sourceService' in request body!`);
        else if (!type.string(data.dataSetName)) response.status(400).send(`Missing parameter or invalid 'dataSetName' in request body!`);
        else if (!type.number(data.minFreeMemory)) response.status(400).send(`Missing parameter or invalid 'minFreeMemory' in request body!`);
        else {

            // check if a new dataSet can be created
            // it must have failed or discarded actively
            if (this.dataSet.hasEnded()) {

                // set up the new dataset
                this.dataSet = new DataSet({
                    sourceService: data.sourceService,
                    setName: data.setName,
                    minFreeMemory: data.minFreeMemory,
                });

                // set the data source service
                this.configuration.set('sourceService', data.sourceService);

                // prepare for new data
                this.dataSet.initialize();

                // initialize the 
                this.initialize();
            } else response.status(409).send(`Cannot create new data set since the existing data set has not ended (status '${this.dataSet.getCurrentStatusName()}')`);
        }
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
            case 'initialized':
            case 'configuring':
            case 'loading':
                return {
                    status: this.dataSet.getCurrentStatusName(),
                };

            case 'ready':
                res.status(201).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;

            case 'discarded':
                res.status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;

            case 'failed':
                res.status(409).send({
                    status: this.dataSet.getCurrentStatusName(),
                    err: this.dataSet.err,
                });
                break;

            default:
                res.status(500).send({
                    status: this.dataSet.getCurrentStatusName(),
                });
                break;
        }
    }
}