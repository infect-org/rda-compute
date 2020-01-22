import Module from './Module.js';
import HTTP2Client from '@distributed-systems/http2-client';
import logd from 'logd';
import path from 'path';
import MainModule from './MainModule.js';


const log = logd.module('source-code-manager');




export default class SourceCodeManager {


    constructor() {
        this.modules = new Map();
        this.rootPath = '/';

        // http 2 client
        this.httpClient = new HTTP2Client();


        // expose the linker function to resolve module dependencies
        this.linker = async(specifier, referencingModule) => this.getModule(specifier, referencingModule);
    }





     /**
      * loads source code modules
      *
      * @param      {array}   moduleList  The module list
      */
    async loadSourceTextModules(moduleList) {
        for (const moduleItem of moduleList) {
            let moduleInstance;
            const specifier = path.join(this.rootPath, moduleItem.specifier);
            
            try {
                // create the module
                moduleInstance = new Module({
                    sourceText: moduleItem.sourceText,
                    specifier,
                });
            } catch (err) {
                err.message = `Failed to load Script Source Module '${specifier}' from data source '${this.dataSourceName}': ${err.message}`;
                err.sourceText = moduleItem.sourceText;
                throw err;
            }

            log.debug(`Source-code for data source ${this.dataSourceName} loaded`);

            this.registerModule(specifier, moduleInstance);
        }
    }



    /**
     * register a new module that can be executed
     *
     * @param      {string}  specifier       The specifier
     * @param      {object}  moduleInstance  The module instance
     */
    registerModule(specifier, moduleInstance) {
        specifier = path.join(this.rootPath, specifier);

        moduleInstance.setLinker(this.linker);
        this.modules.set(specifier, moduleInstance);
    }




    async runModule(specifier) {
        const localSpecifier = `main-module-for-specifier-${specifier}`;

        const mainModule = new MainModule({
            specifier:localSpecifier,
            importName: specifier,
        });

        this.registerModule(localSpecifier, mainModule);
        return this.runMainModule(localSpecifier);
    }




    async runMainModule(specifier) {
        const absoluteSpecifier = path.join(this.rootPath, specifier);

        if (!this.modules.has(absoluteSpecifier)) {
            throw new Error(`Cannot load module with the specifier '${specifier}'. Module was not found!`);
        }

        const moduleInstance = this.modules.get(absoluteSpecifier);

        await moduleInstance.load();
        await moduleInstance.link();
        return moduleInstance.evaluate();
    }





    /**
     * loads source code from the remote host, prepares it for execution
     *
     * @param      {sting}    dataSourceHost  The data soruce host
     * @param      {string}   dataSourceName  The data source name
     */
    async load(dataSourceHost, dataSourceName, dataSetIdentifier) {
        this.dataSourceName = dataSourceName;

        const sourceCodeReponse = await this.httpClient.get(`${dataSourceHost}/${dataSourceName}.source-code`)
            .query({
                dataSetIdentifier,
            })
            .expect(200)
            .send();

        const data = await sourceCodeReponse.getData();
        this.loadSourceTextModules(data);
    }



    /**
     * Determines if a modle exists
     *
     * @param      {string}   specifier  The specifier
     * @return     {boolean}  True if module exists, False otherwise.
     */
    hasModule(specifier) {
        const absoluteSpecifier = path.join(this.rootPath, specifier);
        return this.modules.has(absoluteSpecifier);
    }



    /**
     * get a loaded module
     *
     * @param      {string}   specifier          The specifier
     * @param      {object}   referencingModule  The referencing module
     */
    async getModule(specifier, referencingModule) {
        let currentDir = path.dirname(referencingModule.creator.specifier);

        // prefix relative paths wit the root 
        if (!currentDir.startsWith('/')) {
            currentDir = path.join(this.rootPath, currentDir);
        }

        const absoluteSpecifier = path.join(currentDir, specifier);


        if (!this.modules.has(absoluteSpecifier)) {
            throw new Error(`Cannot load module with the specifier '${absoluteSpecifier}'. Module was not found!`);
        }

        const moduleInstance = this.modules.get(absoluteSpecifier);
        await moduleInstance.load(referencingModule);
        return moduleInstance.getModule();
    }
}