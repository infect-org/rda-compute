import vm from 'vm';




export default class Module {


    /**
     * Constructs a new instance.
     *
     * @param      {Object}  arg1             options
     * @param      {string}  arg1.sourceText  The source text
     * @param      {string}  arg1.specifier   module specifier
     */
    constructor({
        sourceText,
        specifier,
        context,
    }) {
        this.specifier = specifier;
        this.sourceText = sourceText;
        this.context = context
    }





    setLinker(linker) {
        this.linker = linker;
    }




    /**
     * Gets the module.
     */
    async getModule() {
        return this.module;
    }



    /**
     * load dependences
     *
     */
    async link() {
        await this.module.link(this.linker);
    }




    /**
     * load an es module, prepare it for execution
     *
     */
    async load() {
        if (this.module) return this.module;

        // compile the module
        const stModule = this.module = new vm.SourceTextModule(this.sourceText, {
            context: this.context,
        });

        // store a reference to self so that we can resolve modules correctly
        stModule.creator = this;

        return stModule;
    }




    /**
     * evaluates the module. this is used only for the module that is the entry
     * point.
     *
     * @return     {Promise}  { description_of_the_return_value }
     */
    async evaluate() {
        
        // the node vm module api is still unstable. the method must be executed when present ...
        if (this.module.instantiate) {
            this.module.instantiate();
        }

        const { result } = await this.module.evaluate();
        return result;
    }
}
