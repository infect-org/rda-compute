'use strict';

import DataSet from '../src/DataSet';
import section from 'section-tests';
import request from 'superagent';
import assert from 'assert';
import log from 'ee-log';




section('Data Set', (section) => {


    section.test('Set up', async() => {
        new DataSet({
            shardIdentifier: 'test'
        });
    });



    section.test('prepare for data', async() => {
        const dataSet = new DataSet({
            shardIdentifier: 'test'
        });
        dataSet.prepareForData();
    });


    section.test('Add values', async() => {
        const dataSet = new DataSet({
            shardIdentifier: 'test'
        });
        dataSet.prepareForData();
        dataSet.addValues([{
            test: 189
        }]);
    });


    section.test('Discard set', async() => {
        const dataSet = new DataSet({
            shardIdentifier: 'test'
        });
        dataSet.prepareForData();
        dataSet.discard()
        assert(dataSet.hasEnded())
    });


    section.test('Fail set', async() => {
        const dataSet = new DataSet({
            shardIdentifier: 'test'
        });
        dataSet.prepareForData();
        dataSet.fail()
        assert(dataSet.hasEnded())
    });
});