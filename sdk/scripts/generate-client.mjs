// Generates the SDK's typed Solana client from the program IDL with Codama.
// Run `anchor idl build` first (see README "TypeScript bindings generated
// from Rust"); the output in src/program/ is never edited by hand.

import { readFileSync } from 'node:fs';
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
