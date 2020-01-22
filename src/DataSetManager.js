import DataSet from './DataSet.js';



export default class DataSetManager {



    constructor({
        registryClient,
        minFreeMemory = 25,
    }) {
        this.registryClient = registryClient;
        this.dataSets = new Map();
    }



    /**
     * sets up a dataset
     *
     * @param      {Object}   arg1                    options
     * @param      {string}   arg1.dataSource         The data source
     * @param      {string}   arg1.shardIdentifier    The shard identifier
     * @param      {string}   arg1.dataSetIdentifier  The data set identifier
     */
    async createDataSet({
        dataSource,
        shardIdentifier,
        dataSetIdentifier,
    }) {
        if (this.hasDataSet(dataSetIdentifier)) {
            throw new Error(`Canont load dataSet '${dataSetIdentifier}', it was already laoded before!`);
        }

        const dataSet = new DataSet({
            dataSource,
            registryClient: this.registryClient,
            shardIdentifier,
            dataSetIdentifier,
        });

        this.dataSets.set(dataSetIdentifier, dataSet);

        await dataSet.initialize();

        return dataSet;
    }



    /**
     * Determines if data set.
     *
     * @param      {string}   dataSetIdentifier  The data set identifier
     * @return     {boolean}  True if data set, False otherwise.
     */
    hasDataSet(dataSetIdentifier) {
        return this.dataSets.has(dataSetIdentifier);
    }



    /**
     * Gets the data set.
     *
     * @param      {string}  dataSetIdentifier  The data set identifier
     * @return     {object}  The data set.
     */
    getDataSet(dataSetIdentifier) {
        return this.dataSets.get(dataSetIdentifier);
    }
}