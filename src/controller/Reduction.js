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
            let filterParameters = {};

            if (data.parameters && data.parameters.length) {
                try {
                    filterParameters = JSON.parse(data.parameters);
                } catch (err) {
                    return request.response().status(400).send(`Failed to parse parameters: ${err.message}`);
                }
            }

            let subRoutines;

            try {
                subRoutines = JSON.parse(data.subRoutines);
            } catch (e) {
                subRoutines = [];
            }

            const dataSets = await Promise.all(data.shards.map(async (shard) => {
                const res = await this.httpClient.post(`${shard.url}/rda-compute.mapping`).expect(201).send({
                    functionName: data.functionName,
                    parameters: filterParameters,
                    subRoutines,
                    dataSetIdentifier: data.dataSetIdentifier,
                });

                return {
                    shard: shard, 
                    mappingResults: await res.getData(),
                };
            }));


            let filteringDuration = 0;
            for (const dataSet of dataSets) {
                filteringDuration += dataSet.mappingResults.timings.filtering;
            }

            const mapperDuration = process.hrtime.bigint()-mapperStart;
            const result = await dataSet.runReducer({
                functionName: data.functionName,
                dataSets,
                options: data.options,
                subRoutines,
            }).catch((err) => {
                log.error(err);
                throw err;
            });

            if (result.timings) {
                result.timings.mapping = Number(mapperDuration)/1000000;
                result.timings.filtering = filteringDuration;
                result.timings.total = Number(process.hrtime.bigint() - mapperStart)/1000000;
            }

            return result;
        }
    }
}