import { Controller } from '@infect/rda-service';
import type from 'ee-types';
import log from 'ee-log';




export default class MappingController extends Controller {


    constructor({
        dataSetManager,
    }) {
        super('mapping');

        this.dataSetManager = dataSetManager;
        this.enableAction('create');
    }




    /**
    * process the data of the currently loaded data set with
    * the function specified in the request body
    */
    async create(request) {
        const data = await request.getData();

        if (!data) request.response().status(400).send(`Missing request body!`);
        else if (!type.object(data)) request.response().status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.functionName)) request.response().status(400).send(`Missing parameter or invalid 'functionName' in request body!`);
        else if (!type.string(data.dataSetIdentifier)) request.response().status(400).send(`Missing parameter or invalid 'dataSetIdentifier' in request body!`);
        else {
            if (!this.dataSetManager.hasDataSet(data.dataSetIdentifier)) {
                return request.response().status(404).send(`Data Set ${ata.dataSetIdentifier} not found!`);
            }

            const dataSet = this.dataSetManager.getDataSet(data.dataSetIdentifier);

            return await dataSet.runMapper(data.functionName, data.parameters, data.subRoutines).catch((err) => {
                log.error(err);
                throw err;
            });
        }
    }
}