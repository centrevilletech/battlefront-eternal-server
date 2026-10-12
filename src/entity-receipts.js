// Both compact input redundancy and standalone receipts share exact membership
// routing; the publication worker owns admission/ACK state when enabled.
export function acknowledgeEntityPackets({client,state,publication,epoch,packetIds}){
 for(const packetId of packetIds){if(publication)publication.receipt('acks',{client,epoch,packetId,sessionIncarnation:state?.publicationIncarnation});else state?.entity?.ack({epoch,packetId});}
}
