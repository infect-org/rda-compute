import { Controller } from '@infect/rda-service';
import type from 'ee-types';
import logd from 'logd';

const log = logd.module('rda-compute-dataset-controller');


export default class DataSetController extends Controller {


    constructor({
        dataSetManager,
    }) {
        super('data-set');

        this.dataSetManager = dataSetManager;

        this.enableAction('create');
        this.enableAction('listOne');
    }




    /**
    * load a data set
    */
    async create(request) {
        const data = await request.getData();

        if (!data) request.response().status(400).send(`Missing request body!`);
        else if (!type.object(data)) request.response().status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.dataSource)) request.response().status(400).send(`Missing parameter or invalid 'dataSource' in request body!`);
        else if (!type.string(data.shardIdentifier)) request.response().status(400).send(`Missing parameter or invalid 'shardIdentifier' in request body!`);
        else if (!type.string(data.dataSetIdentifier)) request.response().status(400).send(`Missing parameter or invalid 'dataSetIdentifier' in request body!`);
        else {

            if (this.dataSetManager.hasDataSet(data.dataSetIdentifier)) {
                request.response().status(409).send(`Cannot create data set for identifier ${data.dataSetIdentifier}. The dataset was created already!`);
            }

            const dataset = await this.dataSetManager.createDataSet({
                dataSource: data.dataSource,
                shardIdentifier: data.shardIdentifier,
                dataSetIdentifier: data.dataSetIdentifier,
                modelPrefix: data.modelPrefix || '',
            });

            await dataset.load();

            return {
                dataSetIdentifier: data.dataSetIdentifier,
            }
        }
    }









    /**
    * since initializing the dataset is asynchronous
    * the cluster controller needs to be able to poll 
    * its status. This endpoint is used to do this.
    * the status is loaded from the dataSet instance
    * initialized in the create method.
    */
    async listOne(request) {
        const dataSetIdentifier = request.getParameter('id');

        if (!this.dataSetManager.hasDataSet(dataSetIdentifier)) {
            return request.response().status(404).send(`Data Set ${dataSetIdentifier} not found!`);
        }

        const dataSet = this.dataSetManager.getDataSet(dataSetIdentifier);

        switch(dataSet.getCurrentStatusName()) {
            case 'created':
            case 'initializing':
            case 'loading':
            case 'ready':
                return {
                    status: dataSet.getCurrentStatusName(),
                    recordCount: dataSet.getRecordCount(),
                };

            case 'loaded':
                request.response().status(201).send({
                    status: dataSet.getCurrentStatusName(),
                    recordCount: dataSet.getRecordCount(),
                });
                break;

            case 'discarded':
                request.response().status(409).send({
                    status: dataSet.getCurrentStatusName(),
                    recordCount: dataSet.getRecordCount(),
                });
                break;

            case 'failed':
                request.response().status(409).send({
                    status: dataSet.getCurrentStatusName(),
                    recordCount: dataSet.getRecordCount(),
                    err: dataSet.err.message,
                    stack: dataSet.err.stack,
                });
                break;

            default:
                request.response().status(500).send({
                    status: dataSet.getCurrentStatusName(),
                    recordCount: dataSet.getRecordCount(),
                });
                break;
        }
    }
}