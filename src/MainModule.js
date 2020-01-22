import Module from './Module.js';


export default class MainModule extends Module {

    constructor({
        importName,
        specifier,
    } = {}) {
        super({
            specifier,
            sourceText:  `import MainModule from '${importName}';\nMainModule`,
        });
    }
}
