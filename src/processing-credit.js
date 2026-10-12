// Credit belongs to the worker that emitted a batch, including across rotation.
export function returnProcessingCredit(matchWorld,message,schedule=queueMicrotask){
 if(message.type==='inputAck'&&message.inputAckBatchId){schedule(()=>matchWorld.postMessage({type:'inputAckBatchProcessed',inputAckBatchId:message.inputAckBatchId}));return true;}
 if(message.type==='publicationBatchDone'){matchWorld.postMessage({...message,type:'publicationBatchProcessed'});return true;}
 return false;
}
