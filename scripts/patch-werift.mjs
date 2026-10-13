// Patch two partial-reliability bugs in the pinned werift (0.25.0), the
// portable WebRTC stack the packaged desktop HOST runs. Upstream has no
// newer release; remove this when one fixes either line.
//
// 1. A data channel's maxPacketLifeTime expiry is stored as
//    `Date.now() + lifetime / 1000` (milliseconds) but compared against
//    `Date.now() / 1000` (seconds), so timed reliability never abandons.
// 2. The retransmit-limit abandon check is `!!chunk.maxRetransmits`, which
//    is false at exactly the value that must abandon soonest: 0.
//
// Together they make the unordered state channel fully reliable on this
// stack, so disposable state retransmits until the session's backlog drop
// starts discarding fresh snapshots instead. Idempotent; fails loudly if
// the bundled entry no longer contains either form.
import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'node_modules', 'werift', 'lib');
const patches = [
  ['Date.now() + channel.maxPacketLifeTime / 1000', 'Date.now() / 1000 + channel.maxPacketLifeTime / 1000'],
  ['Date.now() + channel.maxPacketLifeTime / 1e3', 'Date.now() / 1e3 + channel.maxPacketLifeTime / 1e3'],
  ['!!chunk.maxRetransmits && chunk.maxRetransmits < chunk.sentCount', 'chunk.maxRetransmits !== undefined && chunk.maxRetransmits < chunk.sentCount'],
];
let found = false, changed = 0;
async function walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) await walk(path);
    else if (/\.(?:js|mjs)$/.test(entry.name)) {
      let text = await readFile(path, 'utf8');
      const before = text;
      for (const [from, to] of patches) {
        if (text.includes(to)) found = true;
        if (text.includes(from)) { found = true; text = text.split(from).join(to); }
      }
      if (text !== before) { await writeFile(path, text); changed++; }
    }
  }
}
await walk(root);
if (!found) throw Error('werift patch targets not found; check the pinned werift version before shipping unpatched');
console.log(`werift partial-reliability patches applied (${changed} files updated)`);
