'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';
import {ShardedDataSet} from 'rda-fixtures';



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
        await service.load();


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



        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});