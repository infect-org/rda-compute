import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import path from 'path';
import ServiceManager from '@infect/rda-service-manager';
import DataSet from '../src/DataSet.js';
import RegistryClient from '@infect/rda-service-registry-client'
import RainbowConfig from '@rainbow-industries/rainbow-config';
import { ShardedDataSet } from '@infect/rda-fixtures';



section('Data Set', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('infect-rda-sample-storage');
    });



    section.test('Create & Load', async() => {
        section.setTimeout(5000);

        const configDir = path.join(path.dirname(new URL(import.meta.url).pathname), '../config/');
        const config = new RainbowConfig(configDir);
        await config.load();

        const registryClient = new RegistryClient(config.get('service-registry.host'));
        const fixturesDataSet = new ShardedDataSet();
        const shardIdentifier = await fixturesDataSet.create();

        const dataset = new DataSet({
            shardIdentifier,
            dataSource: 'infect-rda-sample-storage',
            registryClient,
            dataSetIdentifier: fixturesDataSet.dataSetId,
        });

        await dataset.initialize();
        await dataset.load();

        while(true) {
            await section.wait(200);
            if (dataset.getCurrentStatusName() !== 'loading') break;
        }

        assert.equal(dataset.getCurrentStatusName(), 'loaded');
        assert.equal(dataset.values.length, 100);
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});