'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';
import {ShardedDataSet} from 'rda-fixtures';



const host = 'http://l.dns.porn';


const mapper = `
    const Mapper = class {
        async compute() {
            return {value: 1};
        }
    }
Mapper`;

const reducer = `
    const Reducer = class {
        async compute(data) {
            return {
                result: data[0].value + 2
            };
        }
    }
Reducer`;



section('Reduction Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster');
    });




    section.test('Reduce Data', async() => {
        section.setTimeout(5000);

        const service = new Service();
        await service.load();


        // add mapper function
        section.notice('registering mapping function');
        const mapperName = 'source-identifier-'+Math.round(Math.random()*100000);
        const storageHost = await service.registryClient.resolve('infect-rda-sample-storage');

        await superagent.post(`${storageHost}/infect-rda-sample-storage.source-code`).ok(res => res.status === 201).send({
            sourceCode: mapper,
            identifier: mapperName,
            type: 'mapper',
        });


        section.notice('registering reducer function');
        await superagent.post(`${storageHost}/infect-rda-sample-storage.source-code`).ok(res => res.status === 201).send({
            sourceCode: reducer,
            identifier: mapperName,
            type: 'reducer',
        });



        // add fixtures
        section.notice('create fixtures');
        const dataSet = new ShardedDataSet();
        const shardName = await dataSet.create();



        section.notice('initialize data set');
        await superagent.post(`${host}:${service.getPort()}/rda-compute.data-set`).ok(res => res.status === 201).send({
            dataSource: 'infect-rda-sample-storage',
            shardIdentifier: shardName,
            minFreeMemory: 25,
        });



        section.notice('get status');
        await superagent.get(`${host}:${service.getPort()}/rda-compute.data-set`).ok(res => res.status === 200).send();


        await section.wait(200);
        section.notice('get completed status');
        await superagent.get(`${host}:${service.getPort()}/rda-compute.data-set`).ok(res => res.status === 201).send();



        section.notice('execute reducer on it');
        const mappingResponse = await superagent.post(`${host}:${service.getPort()}/rda-compute.reduction`).ok(res => res.status === 201).send({
            functionName: mapperName,
            shards: [{
                url: `${host}:${service.getPort()}`
            }]
        });
        
        assert(mappingResponse.body);
        assert.equal(mappingResponse.body.result, 3);

        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});