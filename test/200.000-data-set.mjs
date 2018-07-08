'use strict';

import DataSet from '../src/DataSet';
import section from 'section-tests';
import request from 'superagent';
import assert from 'assert';
import log from 'ee-log';



const host = 'http://l.dns.porn:8000';



section('Data Set', (section) => {

    section.test('Set up', async() => {
        new DataSet();
    });


    section.test('Initiailze', async() => {
        const dataSet = new DataSet();
        dataSet.initialize();
    });


    section.test('Add values', async() => {
        const dataSet = new DataSet();
        dataSet.initialize();
        dataSet.addValues([{
            test: 189
        }]);
    });


    section.test('Discard set', async() => {
        const dataSet = new DataSet();
        dataSet.initialize();
        dataSet.discard()
        assert(dataSet.hasEnded())
    });


    section.test('Fail set', async() => {
        const dataSet = new DataSet();
        dataSet.initialize();
        dataSet.fail()
        assert(dataSet.hasEnded())
    });
});