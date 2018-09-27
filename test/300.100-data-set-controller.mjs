import Service from '../index.mjs';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import HTTP2Client from '@distributed-systems/http2-client';
import { ShardedDataSet } from 'rda-fixtures';



const host = 'http://l.dns.porn';



section('Data Set Controller', (section) => {
    let sm;
    let shardName;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage', 'rda-cluster');


        // add fixtures
        const dataSet = new ShardedDataSet();
        shardName = await dataSet.create();
    });




    section.test('Initialize new Data Set', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();


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



        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});