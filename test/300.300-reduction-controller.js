import Service from '../index.js';
import section from 'section-tests';
import ServiceManager from '@infect/rda-service-manager';
import HTTP2Client from '@distributed-systems/http2-client';
import assert from 'assert';
import log from 'ee-log';
import { ShardedDataSet } from '@infect/rda-fixtures';



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
                result: data[0].results.value + 2
            };
        }
    }
Reducer`;



section('Reduction Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster');
    });




    section.test('Reduce Data', async() => {
        section.setTimeout(15000);

        const service = new Service();
        const client = new HTTP2Client();
        await service.load();


        // add mapper function
        section.notice('registering mapping function');
        const mapperName = 'source-identifier-'+Math.round(Math.random()*100000);
        const storageHost = await service.registryClient.resolve('infect-rda-sample-storage');

        await client.post(`${storageHost}/infect-rda-sample-storage.source-code`).expect(201).send({
            sourceCode: mapper,
            identifier: mapperName,
            type: 'mapper',
        });


        section.notice('registering reducer function');
        await client.post(`${storageHost}/infect-rda-sample-storage.source-code`).expect(201).send({
            sourceCode: reducer,
            identifier: mapperName,
            type: 'reducer',
        });



        // add fixtures
        section.notice('create fixtures');
        const dataSet = new ShardedDataSet();
        const shardName = await dataSet.create();



        section.notice('initialize data set');
        await client.post(`${host}:${service.getPort()}/rda-compute.data-set`).expect(201).send({
            dataSource: 'infect-rda-sample-storage',
            shardIdentifier: shardName,
            minFreeMemory: 25,
        });



        section.notice('get status');
        await client.get(`${host}:${service.getPort()}/rda-compute.data-set`).expect(200).send();


        await section.wait(200);
        section.notice('get completed status');
        await client.get(`${host}:${service.getPort()}/rda-compute.data-set`).expect(201).send();



        section.notice('execute reducer on it');
        const mappingResponse = await client.post(`${host}:${service.getPort()}/rda-compute.reduction`).expect(201).send({
            functionName: mapperName,
            shards: [{
                url: `${host}:${service.getPort()}`
            }]
        });
        const data = await mappingResponse.getData();

        assert(data);
        assert.equal(data.result, 3);

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});