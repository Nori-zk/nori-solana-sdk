// Generates the SDK's typed Solana client from the program IDL with Codama.
// Run `anchor idl build` first (see README "TypeScript bindings generated
// from Rust"); the output in src/program/ is never edited by hand.

import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createFromRoot } from 'codama';
import { rootNodeFromAnchor } from '@codama/nodes-from-anchor';
import { renderVisitor } from '@codama/renderers-js';

const idl = JSON.parse(readFileSync(new URL('../../idl/token.json', import.meta.url), 'utf8'));
const codama = createFromRoot(rootNodeFromAnchor(idl));

await codama.accept(
    renderVisitor(new URL('..', import.meta.url).pathname, {
        generatedFolder: 'src/program',
        importExtension: 'js',
    })
);

// The client runs in browsers, which have no `process`. Codama's error files
// keep their messages only outside production, read from
// `process.env['NODE_ENV']` at load; here the messages are always kept, and
// no generated file reads `process`.
const errorsFolder = new URL('../src/program/errors/', import.meta.url);
for (const file of readdirSync(errorsFolder).filter((name) => name !== 'index.ts')) {
    const path = new URL(file, errorsFolder);
    const source = readFileSync(path, 'utf8');
    const rewritten = source
        .replace(
            /let (\w+): (Record<\w+, string>) \| undefined;\s*if \(process\.env\['NODE_ENV'\] !== 'production'\) \{\s*\1 = (\{[\s\S]*?\n {4}\});\s*\}/,
            'const $1: $2 = $3;'
        )
        .replace(
            /\{\s*if \(process\.env\['NODE_ENV'\] !== 'production'\) \{\s*return \((\w+) as Record<\w+, string>\)\[code\];\s*\}\s*return '[^']*';\s*\}/,
            '{\n    return $1[code];\n}'
        );
    if (rewritten.includes('process.')) throw new Error(`${file} still reads process after generation.`);
    writeFileSync(path, rewritten);
}
