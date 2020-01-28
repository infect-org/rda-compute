import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import HTTP2Client from '@distributed-systems/http2-client';
import { ShardedDataSet } from '@infect/rda-fixtures';



const host = 'http://l.dns.porn';



section('Data Set Controller', (section) => {
    let sm;
    let shardIdentifier;
    let dataSetIdentifier;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('@infect/rda-service-registry');
        await sm.startServices('@infect/infect-rda-sample-storage', '@infect/rda-cluster-service');


        // add fixtures
        const dataSet = new ShardedDataSet();
        shardIdentifier = await dataSet.create();
        dataSetIdentifier = dataSet.dataSetId;
    });




    section.test('Initialize new Data Set', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();


        section.notice('initialize data set');
        await client.post(`${host}:${service.getPort()}/rda-compute.data-set`).expect(201).send({
            dataSource: 'infect-rda-sample-storage',
            shardIdentifier,
            dataSetIdentifier,
            modelPrefix: 'Infect',
        });

        while (true) {
            await section.wait(200);
            const response = await client.get(`${host}:${service.getPort()}/rda-compute.data-set/${dataSetIdentifier}`).send();
            if (response.status(201)) break;
        }

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});