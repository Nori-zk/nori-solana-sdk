import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

/** The sdk's sources, the generated program client included; the tests run in Node only. */
const SRC = new URL('../../', import.meta.url).pathname;

/**
 * Every TypeScript source under `folder`, the tests left out.
 *
 * @param folder The folder.
 * @returns The files' paths.
 */
function sourcesIn(folder: string): string[] {
    return readdirSync(folder).flatMap((name) => {
        const path = join(folder, name);
        if (statSync(path).isDirectory()) return relative(SRC, path) === 'tests' ? [] : sourcesIn(path);
        return path.endsWith('.ts') ? [path] : [];
    });
}

describe('the sdk in a browser', () => {
    test('no source reads `process`, which a browser does not have', () => {
        const reading = sourcesIn(SRC).filter((path) => /\bprocess\s*(\.|\[)/.test(readFileSync(path, 'utf8')));
        expect(reading.map((path) => relative(SRC, path))).toEqual([]);
    });
});
