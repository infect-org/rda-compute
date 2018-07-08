'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';



const host = 'http://l.dns.porn:9000';



section('Data Set Controller', (section) => {

    section.test('Initialize new Data Set', async() => {
        const service = new Service();
        await service.load();

        await superagent.post(`${host}/rda-cpmute-service.data-set`).ok(res => res.status === 201).send({
            sourceService: '',
            versions: [],
            dataSetName: '',
            minFreeMemory: 10,
        }).catch(err => log(err));

        await section.wait(200);
        await service.end();
    });
});