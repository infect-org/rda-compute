'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import log from 'ee-log';



/**
* this controller is used to initialize the service as a compute 
* node with an assignment to a cluster. It gets a name set which
* is later used by the data providing service to distribute the 
* data evenly.
*/



export default class ConfigurationController extends Controller {


    constructor({
        configuration
    }) {
        super('configuration');

        // used to set the services configuration
        this.configuration = configuration;


        this.enableAction('create');
    }





    /**
    * register a new service
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.string(data.name)) response.status(400).send(`Missing parameter or invalid 'name' in request body!`);
        else {

            // set the clusters name
            this.configuration.set('name', data.name);
        }
    }
}