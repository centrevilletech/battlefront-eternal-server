import { monitorEventLoopDelay } from 'node:perf_hooks';

// Timing of the thread that runs the game port, for the probe and the
// operator: how late this thread's event loop runs, and how long a world
// publication waits before its datagrams leave (the snapshot hop).
const loop = monitorEventLoopDelay({ resolution: 1 });
loop.enable();
let cpuStart = process.threadCpuUsage(), cpuAt = performance.now();
const samples = { sendHop: [] };
export const wallNow = () => performance.timeOrigin + performance.now();
export function recordHop(name, value) {
  const list = samples[name] ??= [];
  if (Number.isFinite(value) && list.length < 200000) list.push(value);
}
export function resetNetMetrics() {
  loop.reset();
  cpuStart = process.threadCpuUsage(); cpuAt = performance.now();
  for (const list of Object.values(samples)) list.length = 0;
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
    eventLoopMs: { mean: ms(loop.mean), p50: ms(loop.percentile(50)), p99: ms(loop.percentile(99)), max: ms(loop.max) },
    sendHopMs: summary(samples.sendHop),
  };
}
