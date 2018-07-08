'use strict';


import RDAService from 'rda-service';
import {RegistryClient} from 'rda-service-registry';
import path from 'path';
import logd from 'logd';

const log = logd.module('rda-compute-service');



// controllers
import DataSetController from './controller/DataSet';







export default class ServiceRegistry extends RDAService {


    constructor() {
        super('rda-cpmute-service');


        // get the configuration file
        this.loadConfig(this.dirname());


        // set up the registry client
        this.registryClient = new RegistryClient({
            serviceName: this.name,
            registryHost: this.config.services.serviceRegistry
        });
    }




    /**
    * prepare the service
    */
    async load() {


        // register controllers
        this.registerController(new DataSetController());


        await super.load();


        // tell the service registry that we're up and running
        await this.registryClient.register();
    }





    /**
    * shut down the service
    */
    async end() {
        await this.registryClient.deregister();
        await super.end();
    }






    /**
    * returns the current directory for this class
    */
    dirname() {
        return path.join(path.dirname(new URL(import.meta.url).pathname), '../');
    }
}