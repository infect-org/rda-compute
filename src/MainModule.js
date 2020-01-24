import Module from './Module.js';


export default class MainModule extends Module {

    constructor({
        importName,
        specifier,
        context,
    } = {}) {
        super({
            specifier,
            context,
            sourceText:  `import MainModule from '${importName}';\nMainModule`,
        });
    }
}
