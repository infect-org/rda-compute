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
    }) {
        this.specifier = specifier;
        this.sourceText = sourceText;
    }




    /**
     * set up a context to use in the module. uses the context of the 
     * referencing module if present
     *
     * @param      {object}   referencingModule  The referencing module
     */
    async createContext(referencingModule) {
        if (referencingModule) {
            this.context = referencingModule.context;
        } else {
            this.context = vm.createContext({
                console,
                process,
            });
        }

        return this.context;
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
     * @param      {object}   referencingModule  The referencing module
     */
    async load(referencingModule) {
        if (this.module) return this.module;

        // get a context. if the module is loaded from another, the
        // context of the other module is used
        const context = await this.createContext(referencingModule);

        // compile the module
        const stModule = this.module = new vm.SourceTextModule(this.sourceText, {
            context: context,
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
        const { result } = await this.module.evaluate();
        return result;
    }
}
