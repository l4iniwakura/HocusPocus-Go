import { Server } from '@hocuspocus/server';
import * as Y from 'yjs';
const storage = new Map();
const server = new Server({ address: '127.0.0.1', port: 0, quiet: true,
 onLoadDocument({ documentName, document }) { const state = storage.get(documentName); if (state) Y.applyUpdate(document, state); },
 onStoreDocument({ documentName, document }) { storage.set(documentName, Y.encodeStateAsUpdate(document)); },
});
await server.listen();
console.log(`LISTEN 127.0.0.1:${server.httpServer.address().port}`);
process.on('SIGTERM', async () => { await server.destroy(); process.exit(0); });
