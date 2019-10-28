import vm from 'vm';



export default class Module {

    constructor({
        sourceCode,
    }) {
        this.sourceCode = sourceCode;
    }



    /**
    * execute computation
    */
    async compute(data) {
        const instance = new this.Constructor();
        return await instance.compute(data);
    }



    /**
    * build an es6 module
    */
    async load() {
        this.context = vm.createContext({console});
        this.module = new vm.SourceTextModule(this.sourceCode, {
            context: this.context
        });

        // linking is not supported for the moment
        await this.module.link(async () => {});


        // the api in node v12 removed this method ...
        if (typeof this.module.instantiate === 'function') {
            this.module.instantiate();
        }

        const {result} = await this.module.evaluate();
        this.Constructor = result;
    }
}