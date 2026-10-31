// Identical external wire workload, separate server processes, no database/network service.
import { spawn, execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { WebSocket } from 'ws';
import * as Y from 'yjs';
import * as encoding from 'lib0/encoding';
import * as decoding from 'lib0/decoding';

const encoder = (name, type, build) => { const e = encoding.createEncoder(); encoding.writeVarString(e, name); encoding.writeVarUint(e, type); build?.(e); return encoding.toUint8Array(e); };
const source = new Y.Doc(); const updates = [];
source.on('update', update => updates.push(update));
for (let n = 0; n < 500; n++) source.getText('body').insert(n, 'x');
function rss(pid) {
 if (process.platform === 'win32') return Number(execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', `(Get-Process -Id ${pid}).WorkingSet64`], { encoding: 'utf8' }).trim());
 if (process.platform === 'linux') return Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, 'utf8'))[1]) * 1024;
 return Number(execFileSync('ps', ['-o', 'rss=', '-p', String(pid)], { encoding: 'utf8' }).trim()) * 1024;
}
async function scenario(runtime, documents) {
 const executable = process.env.HOCUSPOCUS_GO_BINARY;
 if (!executable) throw new Error('HOCUSPOCUS_GO_BINARY required');
 const child = runtime === 'Go' ? spawn(executable, ['-listen', '127.0.0.1:0']) : spawn(process.execPath, ['tools/benchmark-node.mjs']);
 const sockets = [];
 try {
  const url = await new Promise((accept,reject) => { const timer = setTimeout(() => reject(new Error('startup timeout')), 15000); child.once('error', reject); child.stdout.on('data', bytes => { const found = /LISTEN (\S+)/.exec(String(bytes)); if (found) { clearTimeout(timer); accept(`ws://${found[1]}`); } }); });
  const idleMiB = rss(child.pid) / 1048576;
  let acknowledgements = 0; let allAcknowledged, rejectCompletion;
  const completed = new Promise((resolve,reject) => { allAcknowledged = resolve; rejectCompletion = reject; });
  await Promise.all(Array.from({ length: documents }, (_, index) => new Promise((accept,reject) => {
   const name = `benchmark-${index}`; const socket = new WebSocket(url); sockets.push(socket);
   socket.once('error', reject);
   socket.on('open', () => socket.send(encoder(name, 2, e => { encoding.writeVarUint(e, 0); encoding.writeVarString(e, ''); })));
   socket.on('message', bytes => { const d = decoding.createDecoder(new Uint8Array(bytes)); decoding.readVarString(d); const kind = decoding.readVarUint(d);
    if (kind === 2 && decoding.readVarUint(d) === 2) accept();
    if (kind === 8) { if (decoding.readVarUint(d) !== 1) return rejectCompletion(new Error('update rejected')); acknowledgements++; if (acknowledgements === documents * updates.length) allAcknowledged(); }
   });
  })));
  const start = performance.now();
  for (let index = 0; index < documents; index++) for (const update of updates) sockets[index].send(encoder(`benchmark-${index}`, 0, e => { encoding.writeVarUint(e, 2); encoding.writeVarUint8Array(e, update); }));
  const timer = setTimeout(() => rejectCompletion(new Error('ACK timeout')), 30000);
  await completed; clearTimeout(timer);
  const elapsedMs = performance.now() - start; const activeMiB = rss(child.pid) / 1048576;
  return { runtime, documents, updates: acknowledgements, elapsedMs: Math.round(elapsedMs), updatesPerSecond: Math.round(acknowledgements / elapsedMs * 1000), idleMiB: +idleMiB.toFixed(1), activeMiB: +activeMiB.toFixed(1) };
 } finally { for (const socket of sockets) socket.terminate(); child.kill(); }
}
const results = [];
for (const documents of [1, 8, 32]) for (const runtime of ['TypeScript', 'Go']) {
 const result = await scenario(runtime, documents); results.push(result); console.log(JSON.stringify(result));
}
mkdirSync('.data', { recursive: true });
writeFileSync(resolve('.data/benchmark.json'), JSON.stringify({ platform: process.platform, node: process.version, description: '500 incremental text updates per independent document; memory persistence; all sync ACKs awaited; no backend or PostgreSQL', results }, null, 2));
