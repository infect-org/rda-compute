import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import Module from '../src/Module.js';


section('Module', (section) => {

    section.test('Load & Run', async() => {
        const myModule = new Module({
            specifier: 'a.js',
            sourceText: `console.log('hi');`,
        });


        myModule.setLinker(() => {});
        await myModule.load();
        await myModule.link();
    });


    section.test('Evaluate', async() => {
        const myModule = new Module({
            specifier: 'a.js',
            sourceText: `class A {getValue() {return 5;}}\nA`,
        });

        myModule.setLinker(() => {});
        await myModule.load();
        await myModule.link();

        const A = await myModule.evaluate();
        const a = new A();
        assert.equal(a.getValue(), 5);
    });
});