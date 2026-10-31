import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import * as Y from 'yjs';
import { yDocToProsemirrorJSON } from 'y-prosemirror';

function normalize(value) {
 if (Array.isArray(value)) return value.map(normalize);
 if (!value || typeof value !== 'object') return value;
 return Object.fromEntries(Object.entries(value).filter(([k, v]) => !((k === 'attrs' || k === 'content' || k === 'marks') && Object.keys(v).length === 0)).map(([k, v]) => [k, k === 'marks' ? [...v].sort((a,b) => a.type.localeCompare(b.type)).map(normalize) : normalize(v)]));
}
const fixture = { type: 'doc', content: [
 { type: 'heading', attrs: { level: 2 }, content: [{ type: 'text', text: 'Русский 👋' }] },
 { type: 'paragraph', content: [{ type: 'text', text: 'bold', marks: [{ type: 'bold' }, { type: 'link', attrs: { href: 'https://example.org' } }] }, { type: 'text', text: ' plain' }] },
 { type: 'table', content: [{ type: 'tableRow', content: [{ type: 'tableCell', attrs: { colspan: 2, colwidth: [120, 140] }, content: [{ type: 'paragraph' }] }] }] },
 { type: 'inlineMath', attrs: { latex: 'x^2' } },
] };
function convert(value) {
 assert.ok(process.env.HOCUSPOCUS_CODEC_BINARY, 'HOCUSPOCUS_CODEC_BINARY required');
 const result = spawnSync(process.env.HOCUSPOCUS_CODEC_BINARY, [], { input: JSON.stringify(value), encoding: 'utf8', timeout: 10000 });
 assert.equal(result.status, 0, result.stderr);
 return JSON.parse(result.stdout);
}
test('Go transformer creates state accepted by original Yjs and y-prosemirror', () => {
 const result = convert({ document: fixture });
 const doc = new Y.Doc(); Y.applyUpdate(doc, Buffer.from(result.binary, 'base64'));
 assert.deepEqual(normalize(yDocToProsemirrorJSON(doc)), normalize(fixture));
 const reloaded = convert({ binary: Buffer.from(Y.encodeStateAsUpdate(doc)).toString('base64') });
 assert.deepEqual(normalize(reloaded.document), normalize(fixture));
 doc.destroy();
});
