// What a server browser shows of a server beyond how to reach it: the
// operator's description, sponsor and location (config.json), the battle it
// plays (the map, its name, era and mode, and the query a player's page
// loads for it), its rules (bots, assigned teams) and how long it has run.
// The online listing (relayed by the master) and /info (LAN, JOIN IP) both
// carry it; a player's desktop measures the ping itself.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const ERAS = { rep: 'cw', cis: 'cw', all: 'gcw', imp: 'gcw' };
const clip = (value, length) => typeof value === 'string' && value.trim() ? value.replace(/[\x00-\x1f\x7f]/g, ' ').trim().slice(0, length) : null;

// An https address (no credentials in it), or null: a player's browser
// opens and fetches sponsors' links and images from nothing else.
export function httpsUrl(value) {
  if (typeof value !== 'string' || value.length > 512) return null;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && !url.username && !url.password ? url.href : null;
  } catch { return null; }
}

// config.sponsor: { name, url, logo }; a sponsor needs a name.
export function sponsorOf(value) {
  if (!value || typeof value !== 'object') return null;
  const name = clip(value.name, 60);
  return name ? { name, url: httpsUrl(value.url), logo: httpsUrl(value.logo) } : null;
}

// Map ids to the names the front end shows ("BESPIN: CLOUD CITY"), from the
// prepared catalog in the game data; none where it is missing.
export function mapNames(data) {
  try { return Object.fromEntries(JSON.parse(readFileSync(join(data, 'common', 'shell', 'shell.json'), 'utf8')).maps.map((map) => [map.id, map.name])); }
  catch { return {}; }
}

// config: the battle's configuration (its rotation starts with this battle);
// query: the battle's own; startedAt: when the service started (ms).
export function browserInfo({ config, query, startedAt, names = {}, now = Date.now() }) {
  const params = new URLSearchParams(query), map = params.get('map'), side = params.get('side');
  const mode = String(config.rotation?.[0] ?? '').split(':')[2] || 'conquest';
  return {
    description: clip(config.description, 280),
    sponsor: sponsorOf(config.sponsor),
    location: clip(config.location, 80),
    query,
    battle: { map, mapName: clip(names[map], 60), side, era: ERAS[side] ?? null, mode },
    bots: params.get('ai') !== 'off',
    autoAssignTeams: config.autoAssignTeams === true,
    uptime: Math.max(0, Math.floor((now - startedAt) / 1000)),
  };
}
