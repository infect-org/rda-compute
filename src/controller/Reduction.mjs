'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';
import superagent from 'superagent';




export default class ReductionController extends Controller {


    constructor({
        configuration,
    }) {
        super('reduction');

        // service wide configurations received by the cluster 
        // service
        this.configuration = configuration;
        this.enableAction('create');
    }




    /**
    * instruct the shards to map the data, then reduce it
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.functionName)) response.status(400).send(`Missing parameter or invalid 'functionName' in request body!`);
        else if (!type.array(data.shards)) response.status(400).send(`Missing parameter or invalid 'shards' in request body!`);
        else {

            // collect the data from all shards
            const mapperStart = Date.now();
            const dataSets = await Promise.all(data.shards.map(async (shard) => {
                const res = await superagent.post(`${shard.url}/rda-compute.mapping`).send({
                    functionName: data.functionName,
                    parameters: data.parameters,
                });

                return {
                    shard:shard, 
                    results: res.body,
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