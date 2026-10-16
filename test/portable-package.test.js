import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, copyFile, writeFile, symlink, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
// The desktop HOST runs this server under Electron's Node from a copied
// payload: it must load with only its pure JavaScript dependency (ws). The
// transport itself is the mounted engine's (src/net), so no native module,
// WebRTC stack or OpenSSL binary is involved at all.
test('server payload loads with only ws installed', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bfe-portable-package-'));
  try {
    await mkdir(join(root, 'src')); await mkdir(join(root, 'node_modules'));
    await writeFile(join(root, 'package.json'), '{"type":"module"}');
    for (const file of await readdir(new URL('../src/', import.meta.url))) if (file.endsWith('.js')) await copyFile(new URL('../src/' + file, import.meta.url), join(root, 'src', file));
    await symlink(resolve('node_modules', 'ws'), join(root, 'node_modules', 'ws'), 'junction');
    for (const file of ['udp-server.js', 'udp-peer.js', 'local-server.js', 'online-server.js', 'game-session.js'])
      assert.ok(await import(pathToFileURL(join(root, 'src', file))));
  } finally { await rm(root, { recursive: true, force: true }); }
});
