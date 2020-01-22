import { Controller } from '@infect/rda-service';
import type from 'ee-types';
import log from 'ee-log';
import HTTP2Client from '@distributed-systems/http2-client';




export default class ReductionController extends Controller {


    constructor({
        dataSetManager,
    }) {
        super('reduction');

        this.dataSetManager = dataSetManager;
        this.enableAction('create');

        // http 2 client
        this.httpClient = new HTTP2Client();
    }



    /**
     * shut down the class
     */
    async end() {
        await this.httpClient.end();
    }



    /**
    * instruct the shards to map the data, then reduce it
    */
    async create(request) {
        const data = await request.getData();

        if (!data) request.response().status(400).send(`Missing request body!`);
        else if (!type.object(data)) request.response().status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.functionName)) request.response().status(400).send(`Missing parameter or invalid 'functionName' in request body!`);
        else if (!type.array(data.shards)) request.response().status(400).send(`Missing parameter or invalid 'shards' in request body!`);
        else if (!type.string(data.dataSetIdentifier)) request.response().status(400).send(`Missing parameter or invalid 'dataSetIdentifier' in request body!`);
        else {
            if (!this.dataSetManager.hasDataSet(data.dataSetIdentifier)) {
                return request.response().status(404).send(`Data Set ${ata.dataSetIdentifier} not found!`);
            }

            const dataSet = this.dataSetManager.getDataSet(data.dataSetIdentifier);

            // collect the data from all shards
            const mapperStart = process.hrtime.bigint();
            const dataSets = await Promise.all(data.shards.map(async (shard) => {
                const res = await this.httpClient.post(`${shard.url}/rda-compute.mapping`).send({
                    functionName: data.functionName,
                    parameters: data.parameters,
                    dataSetIdentifier: data.dataSetIdentifier,
                });

                return {
                    shard: shard, 
                    mappingResults: await res.getData(),
                };
            }));

            const mapperDuration = process.hrtime.bigint()-mapperStart;
            const result = await dataSet.runReducer(data.functionName, dataSets);

            if (result.timings) {
                result.timings.filtering = Number(mapperDuration)/1000000;
                result.timings.total = Number(process.hrtime.bigint() - mapperStart)/1000000;
            }

            return result;
        }
    }
}