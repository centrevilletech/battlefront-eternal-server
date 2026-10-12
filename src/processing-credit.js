// Cache only after every synchronous session consumer has processed the batch.
// The captured worker owns credit even if the global match rotates meanwhile.
export function returnProcessingCredit(matchWorld,message,schedule=queueMicrotask){
 if(message.type==='inputAck'&&message.inputAckBatchId){schedule(()=>{
  const credit={type:'inputAckBatchProcessed',inputAckBatchId:message.inputAckBatchId,inputAckIncarnation:message.inputAckIncarnation};
  matchWorld.inputAckProcessed=credit;matchWorld.postMessage(credit);
 });return true;}
 if(message.type==='inputAckProbe'){
  const last=matchWorld.inputAckProcessed;
  matchWorld.postMessage({type:'inputAckProbeResult',inputAckBatchId:message.inputAckBatchId,inputAckIncarnation:message.inputAckIncarnation,processed:Boolean(last&&last.inputAckBatchId===message.inputAckBatchId&&last.inputAckIncarnation===message.inputAckIncarnation)});return true;
 }
 if(message.type==='publicationBatchDone'){matchWorld.postMessage({...message,type:'publicationBatchProcessed'});return true;}
 return false;
}
