import RDAService from 'rda-service';
import path from 'path';
import logd from 'logd';

const log = logd.module('rda-compute-service');



// controllers
import DataSetController from './controller/DataSet';
import MappingController from './controller/Mapping';
import ReductionController from './controller/Reduction';



export default class ComputeService extends RDAService {


    constructor() {
        super('rda-compute');

        // configuration values passed by the 
        // cluster service
        this.configuration = new Map();
    }





    /**
    * prepare the service
    */
    async load() {

        const options = {
            configuration: this.configuration,
            registryClient: this.registryClient,
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