import { Controller } from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';
import HTTP2Client from '@distributed-systems/http2-client';




export default class ReductionController extends Controller {


    constructor({
        configuration,
    }) {
        super('reduction');

        // service wide configurations received by the cluster 
        // service
        this.configuration = configuration;
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
        else {

            // collect the data from all shards
            const mapperStart = Date.now();
            const dataSets = await Promise.all(data.shards.map(async (shard) => {
                const res = await this.httpClient.post(`${shard.url}/rda-compute.mapping`).send({
                    functionName: data.functionName,
                    parameters: data.parameters,
                });

                return {
                    shard: shard, 
                    results: await res.getData(),
                };
            }));
            const mapperDuration = Date.now()-mapperStart;

            const reducer = this.configuration.get('sourceCode').get(data.functionName).reducer;
            const result = await reducer.compute(dataSets);


            if (result.timings) {
                result.timings.filtering = mapperDuration;
                result.timings.total = Date.now() - mapperStart;
            }

            return result;
        }
    }
}