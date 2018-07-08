'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';
import DataSet from '../DataSet';




export default class DataSetController extends Controller {


    constructor() {
        super('data-set');

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
    * load data for the current data set, it should
    * already be initialized and have a valid status
    */
    loadData() {

    }






    /**
    * register a new service
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.sourceService)) response.status(400).send(`Missing parameter or invalid 'sourceService' in request body!`);
        else if (!type.string(data.dataSetName)) response.status(400).send(`Missing parameter or invalid 'dataSetName' in request body!`);
        else if (!type.array(data.versions)) response.status(400).send(`Missing parameter or invalid 'versions' in request body!`);
        else if (!type.number(data.minFreeMemory)) response.status(400).send(`Missing parameter or invalid 'minFreeMemory' in request body!`);
        else {

            // check if a new dataSet can be created
            // it must have failed or discarded actively
            if (this.dataSet.hasEnded()) {

                // set up the new dataset
                this.dataSet = new DataSet({
                    sourceService: data.sourceService,
                    versions: data.versions,
                    setName: data.setName,
                    minFreeMemory: data.minFreeMemory,
                });

                // prepare for new data
                this.dataSet.initialize();

                // start loading data
                this.loadData();
            } else response.status(409).send(`Cannot create new data set since the existing data set has not ended (status '${this.dataSet.getCurrentStatusName()}')`);
        }
    }
}