import {monitorEventLoopDelay,performance} from 'node:perf_hooks';
export const parentDiagnosticsEnabled=process.env.BFE_PARENT_DIAGNOSTICS==='1';
let counts={},timings={},started=false;
export function parentCount(kind,amount=1){if(parentDiagnosticsEnabled)counts[kind]=(counts[kind]??0)+amount;}
export function parentDuration(kind,begun){if(!parentDiagnosticsEnabled)return;const ms=performance.now()-begun,t=timings[kind]??={count:0,totalMs:0,maxMs:0};t.count++;t.totalMs+=ms;t.maxMs=Math.max(t.maxMs,ms);}
export function startParentDiagnostics(){
 if(!parentDiagnosticsEnabled||started)return;started=true;
 const delay=monitorEventLoopDelay({resolution:10});delay.enable();
 let previous=performance.now(),cpu=process.cpuUsage(),elu=performance.eventLoopUtilization();
 const timer=setInterval(()=>{
  const now=performance.now(),nextCpu=process.cpuUsage(),nextElu=performance.eventLoopUtilization();
  console.error('parent-diagnostics',JSON.stringify({atUnixMs:Date.now(),elapsedMs:now-previous,cpuUserUs:nextCpu.user-cpu.user,cpuSystemUs:nextCpu.system-cpu.system,eventLoop:performance.eventLoopUtilization(nextElu,elu),delayMs:{p50:delay.percentile(50)/1e6,p95:delay.percentile(95)/1e6,max:delay.max/1e6},memory:process.memoryUsage(),counts,timings}));
  previous=now;cpu=nextCpu;elu=nextElu;counts={};timings={};delay.reset();
 },500);timer.unref();
}
