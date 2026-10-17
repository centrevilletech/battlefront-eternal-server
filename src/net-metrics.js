import { monitorEventLoopDelay } from 'node:perf_hooks';

// Timing of the thread that runs the game port, for the probe and the
// operator: how late this thread's event loop runs, and how long a world
// publication waits before its datagrams leave (the snapshot hop).
// A 1 ms event-loop monitor wakes this thread a thousand times a second, so
// it (and the hop samples) only run once metrics are enabled: by the
// network thread's `metrics` option or by the first reset (the probe's
// measurement window). Counters are always kept; they cost nothing.
let loop = null;
let cpuStart = process.threadCpuUsage(), cpuAt = performance.now();
const samples = { sendHop: [] };
const counters = { stalePublications: 0, supersededSnapshots: 0, inputsDropped: 0, oversizedMessages: 0, sessionFaults: 0 };
export const wallNow = () => performance.timeOrigin + performance.now();
export function enableNetMetrics() {
  if (loop) return;
  loop = monitorEventLoopDelay({ resolution: 1 });
  loop.enable();
}
export function recordHop(name, value) {
  if (!loop) return;
  const list = samples[name] ??= [];
  if (Number.isFinite(value) && list.length < 200000) list.push(value);
}
export function countNet(name, amount = 1) { counters[name] = (counters[name] ?? 0) + amount; }
export function resetNetMetrics() {
  enableNetMetrics();
  loop.reset();
  cpuStart = process.threadCpuUsage(); cpuAt = performance.now();
  for (const list of Object.values(samples)) list.length = 0;
  for (const key of Object.keys(counters)) counters[key] = 0;
}
const round = value => Math.round(value * 1000) / 1000;
function summary(list) {
  if (!list.length) return { count: 0 };
  const sorted = [...list].sort((a, b) => a - b), at = p => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
  return { count: sorted.length, p50: round(at(0.5)), p95: round(at(0.95)), p99: round(at(0.99)), max: round(sorted.at(-1)) };
}
export function netMetrics() {
  const ms = ns => round(ns / 1e6);
  const cpu = process.threadCpuUsage(cpuStart), seconds = (performance.now() - cpuAt) / 1000;
  return {
    // Share of one core this thread used since the last reset.
    threadCores: seconds > 0 ? round((cpu.user + cpu.system) / 1e6 / seconds) : null,
    eventLoopMs: loop ? { mean: ms(loop.mean), p50: ms(loop.percentile(50)), p99: ms(loop.percentile(99)), max: ms(loop.max) } : null,
    sendHopMs: summary(samples.sendHop),
    ...counters,
  };
}
