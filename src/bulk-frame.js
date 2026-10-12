// Negotiated binary-bulk-v1 frames. All integer fields use little endian.
export const BULK_MAGIC=0x31424642;
export const JOURNAL_MAGIC=0x32424642, EVENTS_MAGIC=0x33424642, TRANSIENTS_MAGIC=0x34424642;
export function encodeBulkFrame({id,index,total,data,metadata,magic=BULK_MAGIC}) {
 if(![BULK_MAGIC,JOURNAL_MAGIC,EVENTS_MAGIC,TRANSIENTS_MAGIC].includes(magic))throw Error('Invalid binary kind');
 const body=Buffer.from(data),meta=metadata===undefined?Buffer.alloc(0):Buffer.from(JSON.stringify(metadata));
 if(!Number.isInteger(id)||id<0||id>0xffffffff||!Number.isInteger(index)||!Number.isInteger(total)||total<1||total>128||index<0||index>=total||meta.length>8192||index!==0&&meta.length||body.length>20000)throw Error('Invalid binary bulk frame');
 const frame=Buffer.allocUnsafe(16+meta.length+body.length);
 frame.writeUInt32LE(magic,0);frame.writeUInt32LE(id,4);frame.writeUInt16LE(index,8);frame.writeUInt16LE(total,10);frame.writeUInt32LE(meta.length,12);
 meta.copy(frame,16);body.copy(frame,16+meta.length);return frame;
}
export function decodeBulkFrame(value) {
 const frame=Buffer.from(value);if(frame.length<16||frame.length>32768||![BULK_MAGIC,JOURNAL_MAGIC,EVENTS_MAGIC,TRANSIENTS_MAGIC].includes(frame.readUInt32LE(0)))throw Error('Invalid binary bulk header');
 const id=frame.readUInt32LE(4),index=frame.readUInt16LE(8),total=frame.readUInt16LE(10),length=frame.readUInt32LE(12);
 if(!total||total>128||index>=total||length>8192||index!==0&&length||16+length>frame.length||frame.length-16-length>20000)throw Error('Invalid binary bulk bounds');
 const metadata=length?JSON.parse(frame.toString('utf8',16,16+length)):undefined;
 if(metadata!==undefined&&(!metadata||typeof metadata!=='object'||Array.isArray(metadata)))throw Error('Invalid binary bulk metadata');
 return{magic:frame.readUInt32LE(0),id,index,total,metadata,data:frame.subarray(16+length),encoding:'deflate'};
}
