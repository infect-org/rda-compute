import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import SourceCodeManager from '../src/SourceCodeManager.js';


section('SourceCodeManager', (section) => {

    section.test('Load & Run', async() => {
        const manager = new SourceCodeManager();

        await manager.loadSourceTextModules([{
            sourceText: `import B from './b.js';\nclass A {getValue() {return new B().getValue();}}\nA`,
            specifier: 'main.js',
        }, {
            sourceText: `import C from './c.js';\nexport default class B {getValue() {return new C().getValue();}}`,
            specifier: 'b.js',
        }, {
            sourceText: `export default class C {getValue() {return 5;}}`,
            specifier: 'c.js',
        }]);


        const A = await manager.runMainModule('main.js');
        const a = new A();

        assert.equal(a.getValue(), 5);
    });


    section.test('runModule', async() => {
        const manager = new SourceCodeManager();

        await manager.loadSourceTextModules([{
            sourceText: `import B from './b.js';\nexport default class A {getValue() {return new B().getValue();}}`,
            specifier: 'main.js',
        }, {
            sourceText: `import C from './c.js';\nexport default class B {getValue() {return new C().getValue();}}`,
            specifier: 'b.js',
        }, {
            sourceText: `export default class C {getValue() {return 5;}}`,
            specifier: 'c.js',
        }]);


        const A = await manager.runModule('main.js');
        const a = new A();

        assert.equal(a.getValue(), 5);
    });
});