'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';



const host = 'http://l.dns.porn';



section('Configuration Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
    });




    section.test('Set configuration', async() => {
        const service = new Service();
        await service.load();

        await superagent.post(`${host}:${service.getPort()}/rda-compute.configuration`).ok(res => res.status === 201).send({
            name: 'test'
        });

        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});