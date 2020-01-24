import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import path from 'path';
import ServiceManager from '@infect/rda-service-manager';
import DataSetManager from '../src/DataSetManager.js';
import RegistryClient from '@infect/rda-service-registry-client'
import RainbowConfig from '@rainbow-industries/rainbow-config';
import { ShardedDataSet } from '@infect/rda-fixtures';



section('Data Set Manager', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('@infect/rda-service-registry');
        await sm.startServices('@infect/infect-rda-sample-storage');
    });



    section.test('Load, cerate Data Set', async() => {
        section.setTimeout(5000);

        const configDir = path.join(path.dirname(new URL(import.meta.url).pathname), '../config/');
        const config = new RainbowConfig(configDir);
        await config.load();

        const registryClient = new RegistryClient(config.get('service-registry.host'));
        const fixturesDataSet = new ShardedDataSet();
        const shardIdentifier = await fixturesDataSet.create();

        const manager = new DataSetManager({
            registryClient,
        });


        const dataset = await manager.createDataSet({
            dataSource: 'infect-rda-sample-storage',
            shardIdentifier,
            dataSetIdentifier: fixturesDataSet.dataSetId,
            modelPrefix: 'Infect',
        });

        await dataset.load();

        while(true) {
            await section.wait(200);
            if (dataset.getCurrentStatusName() !== 'loading') break;
        }

        assert.equal(dataset.getCurrentStatusName(), 'loaded');
        assert.equal(dataset.values.length, 100);
        assert.equal(manager.hasDataSet(fixturesDataSet.dataSetId), true);
        assert.equal(manager.getDataSet(fixturesDataSet.dataSetId), dataset);
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});