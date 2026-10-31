import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { Server } from '@hocuspocus/server';
import { Redis } from '@hocuspocus/extension-redis';
import { HocuspocusProvider } from '@hocuspocus/provider';
import { WebSocket } from 'ws';
import * as Y from 'yjs';

const until = async (check) => { const end = Date.now() + 10000; while (Date.now() < end) { if (check()) return; await new Promise(r => setTimeout(r, 10)); } assert.fail('cross-language state did not converge'); };
test('Go and original TypeScript Hocuspocus exchange state through the original Redis protocol', { timeout: 30000 }, async () => {
 assert.ok(process.env.TEST_REDIS_ADDR, 'TEST_REDIS_ADDR required');
 assert.ok(process.env.HOCUSPOCUS_GO_BINARY, 'HOCUSPOCUS_GO_BINARY required');
 const [host, port] = process.env.TEST_REDIS_ADDR.split(':');
 const prefix = `hp-cross-${Date.now()}`;
 const original = new Server({ address: '127.0.0.1', port: 0, quiet: true, extensions: [new Redis({ host, port: Number(port), prefix, disconnectDelay: 100 })] });
 const peers = []; let go;
 try {
  await original.listen(); const originalURL = `ws://127.0.0.1:${original.httpServer.address().port}`;
  go = spawn(process.env.HOCUSPOCUS_GO_BINARY, ['-listen', '127.0.0.1:0', '-redis', process.env.TEST_REDIS_ADDR, '-redis-prefix', prefix], { stdio: ['ignore', 'pipe', 'pipe'] });
  const goURL = await new Promise((resolve,reject) => { const timeout = setTimeout(() => reject(new Error('Go startup timeout')), 15000); go.once('error', reject); go.stdout.on('data', bytes => { const found = /LISTEN (\S+)/.exec(String(bytes)); if (found) { clearTimeout(timeout); resolve(`ws://${found[1]}`); } }); });
  const create = (url) => { const doc = new Y.Doc(); const provider = new HocuspocusProvider({ url, name: 'note', document: doc, WebSocketPolyfill: WebSocket }); peers.push(provider); return { doc, provider, text: doc.getText('body') }; };
  const a = create(originalURL); await until(() => a.provider.synced); a.text.insert(0, 'from TypeScript');
  const b = create(goURL); await until(() => b.provider.synced && b.text.toString() === a.text.toString());
  b.text.insert(0, 'from Go; '); await until(() => a.text.toString() === 'from Go; from TypeScript');
  a.text.delete(0, 4); await until(() => a.text.toString() === b.text.toString());
  a.provider.awareness.setLocalStateField('user', { name: 'Cross language' });
  await until(() => [...b.provider.awareness.getStates().values()].some(v => v.user?.name === 'Cross language'));
 } finally { for (const peer of peers) peer.destroy(); go?.kill(); await original.destroy(); }
});
