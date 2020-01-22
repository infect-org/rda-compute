import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import HTTP2Client from '@distributed-systems/http2-client';
import { ShardedDataSet } from '@infect/rda-fixtures';



const host = 'http://l.dns.porn';



section('Reducing Controller', (section) => {
    let sm;
    let shardIdentifier;
    let dataSetIdentifier;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster');


        // add fixtures
        const dataSet = new ShardedDataSet();
        shardIdentifier = await dataSet.create();
        dataSetIdentifier = dataSet.dataSetId;
    });




    section.test('Reduce', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();


        section.notice('initialize data set');
        await client.post(`${host}:${service.getPort()}/rda-compute.data-set`).expect(201).send({
            dataSource: 'infect-rda-sample-storage',
            shardIdentifier,
            dataSetIdentifier,
        });

        while (true) {
            await section.wait(200);
            const response = await client.get(`${host}:${service.getPort()}/rda-compute.data-set/${dataSetIdentifier}`).send();
            if (response.status(201)) break;
        }


        section.notice('reduction');
        const response = await client.post(`${host}:${service.getPort()}/rda-compute.reduction`).expect(201).send({
            dataSetIdentifier,
            functionName: 'Infect',
            parameters: {},
            shards: [{
                url: `${host}:${service.getPort()}`
            }]
        });

        const data = await response.getData();

        assert(data.values.length > 0);
        assert.equal(data.counters.filteredModelCount, 0);
        assert.equal(data.counters.invalidModelCount, 0);
        assert.equal(data.counters.totalModelCount, 100);

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});