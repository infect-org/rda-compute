'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import request from 'superagent';
import assert from 'assert';
import log from 'ee-log';



const host = 'http://l.dns.porn:8000';



section('RDA Compute Service', (section) => {

    section.test('Start & stop service', async() => {
        const service = new Service();

        await service.load();
        await section.wait(200);
        await service.end();
    });
});