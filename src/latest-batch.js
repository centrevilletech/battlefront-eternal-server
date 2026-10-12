import {ProcessingLane} from './processing-lane.js';
export class LatestBatch extends ProcessingLane {
 constructor(send,options={}){super(send,{...options,failureMessage:'input acknowledgement transport processing timed out',describeBatch:({id,incarnation,...batch})=>({type:'inputAck',inputAckBatchId:id,inputAckIncarnation:incarnation,...batch}),describeProbe:({id,incarnation})=>({type:'inputAckProbe',inputAckBatchId:id,inputAckIncarnation:incarnation})});}
 probeResult(message){if(message.processed===true)this.processed(message.inputAckBatchId,message.inputAckIncarnation);}
}
