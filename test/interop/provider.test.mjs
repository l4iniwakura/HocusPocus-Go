import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { HocuspocusProvider, HocuspocusProviderWebsocket } from '@hocuspocus/provider';
import { WebSocket } from 'ws';
import * as Y from 'yjs';

let child, url;
const peers = [];
before(async () => {
 const executable = process.env.HOCUSPOCUS_GO_BINARY;
 assert.ok(executable, 'Build cmd/hocuspocus and set HOCUSPOCUS_GO_BINARY');
 child = spawn(executable, ['-listen', '127.0.0.1:0'], { stdio: ['ignore', 'pipe', 'pipe'] });
 url = await new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('server startup timed out')), 15000);
  child.once('error', reject);
  child.once('exit', code => { clearTimeout(timer); reject(new Error(`server exited: ${code}`)); });
  child.stdout.on('data', data => { const found = /LISTEN (\S+)/.exec(String(data)); if (found) { clearTimeout(timer); resolve(`ws://${found[1]}`); } });
 });
});
after(() => { for (const p of peers) p.destroy(); child?.kill(); });
const until = async (check) => { const end = Date.now() + 5000; while (Date.now() < end) { if (check()) return; await new Promise(r => setTimeout(r, 10)); } assert.fail('condition did not converge'); };
function peer(name, websocketProvider) {
 const document = new Y.Doc();
 const provider = new HocuspocusProvider({ url, name, document, WebSocketPolyfill: WebSocket, ...(websocketProvider ? { websocketProvider } : {}) });
 if (websocketProvider) provider.attach();
 peers.push(provider);
 return { provider, document, text: document.getText('body') };
}
test('real TypeScript provider syncs concurrent text, formatting, deletions and reconnect', { timeout: 20000 }, async () => {
 const a = peer('text'); const b = peer('text');
 await until(() => a.provider.synced && b.provider.synced);
 a.text.insert(0, 'Hello 👋');
 await until(() => b.text.toString() === a.text.toString());
 a.text.insert(0, 'A'); b.text.insert(0, 'B');
 await until(() => a.text.toString() === b.text.toString() && a.text.length === 10);
 a.text.format(0, 2, { bold: true }); a.text.delete(2, 1);
 await until(() => JSON.stringify(a.text.toDelta()) === JSON.stringify(b.text.toDelta()));
 await until(() => !a.provider.hasUnsyncedChanges && !b.provider.hasUnsyncedChanges);
 const c = peer('text'); await until(() => c.provider.synced && c.text.toString() === a.text.toString());
 assert.deepEqual(c.text.toDelta(), a.text.toDelta());
});
test('real provider receives stateless replies and awareness', { timeout: 10000 }, async () => {
 const a = peer('presence'); const b = peer('presence');
 await until(() => a.provider.synced && b.provider.synced);
 a.provider.awareness.setLocalStateField('user', { name: 'Alice' });
 await until(() => [...b.provider.awareness.getStates().values()].some(v => v.user?.name === 'Alice'));
 const reply = new Promise(resolve => a.provider.on('stateless', ({ payload }) => resolve(payload)));
 a.provider.sendStateless('roundtrip'); assert.equal(await reply, 'roundtrip');
});
test('multiple real providers multiplex one WebSocket', { timeout: 10000 }, async () => {
 const socket = new HocuspocusProviderWebsocket({ url, WebSocketPolyfill: WebSocket });
 try {
  const a = peer('multiplex-a', socket); const b = peer('multiplex-b', socket);
  await until(() => a.provider.synced && b.provider.synced);
  a.text.insert(0, 'first'); b.text.insert(0, 'second');
  const c = peer('multiplex-a'); const d = peer('multiplex-b');
  await until(() => c.text.toString() === 'first' && d.text.toString() === 'second');
 } finally { socket.destroy(); }
});
