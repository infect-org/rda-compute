import RDAService from '@infect/rda-service';
import path from 'path';
import logd from 'logd';
import DataSetManager from './DataSetManager.js';

const log = logd.module('rda-compute-service');



// controllers
import DataSetController from './controller/DataSet.js';
import MappingController from './controller/Mapping.js';
import ReductionController from './controller/Reduction.js';



const appRoot = path.join(path.dirname(new URL(import.meta.url).pathname), '../');



export default class ComputeService extends RDAService {


    constructor() {
        super({
            name: 'rda-compute',
            appRoot,
        });
    }





    /**
    * prepare the service
    */
    async load() {
        await this.initialize();

        this.dataSetManager = new DataSetManager({
            registryClient: this.registryClient,
        });

        
        const options = {
            dataSetManager: this.dataSetManager,
        };


        // register controllers
        this.registerController(new DataSetController(options));
        this.registerController(new MappingController(options));
        this.registerController(new ReductionController(options));

        await super.load();


        // tell the service registry that we're up and running
        await this.registerService();
    }
}