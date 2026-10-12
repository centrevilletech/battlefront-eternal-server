// External edge IDs can arrive out of order. Engine mailboxes deliberately use
// monotonic internal IDs; retain the translation until simulation consumption.
export class EntityEdges {
  constructor({Receiver,actor,world,client,registry,capacity=256}) {
    this.actor=actor;this.world=world;this.client=client;this.registry=registry;this.capacity=capacity;
    this.receiver=new Receiver();this.pending=new Map();this.results=new Map();this.received=[];
    actor.actions.enableOutcomes();actor.fire.enableOutcomes();this.reset(1,actor.unit.generation);
  }
  reset(epoch,generation) {
    this.epoch=epoch;this.generation=generation;this.receiver.reset(epoch,generation);
    this.pending.clear();this.results.clear();this.received=[];
    this.actionSerial=this.actor.actions.received;this.fireSerial=this.actor.fire.received;
  }
  result(id,status) {if(!this.results.has(id)&&this.results.size>=this.capacity)throw Error('entity result backlog');this.results.set(id,{id,status});}
  receive(packet,tick) {
    if(packet.epoch!==this.epoch||packet.generation!==this.generation)return;
    if(this.pending.size+this.receiver.queue.size+packet.edges.length>this.capacity)throw Error('entity edge backlog');
    const {receipts,rejected}=this.receiver.receive(packet.edges,tick);
    this.received=[...new Set([...this.received,...receipts])].slice(-32);
    for(const value of rejected)this.result(value.id,value.status==='expired'?'expired':'rejected');
  }
  advance(tick) {
    for(const edge of this.receiver.take(tick)) {
      if(edge.tick<tick-30){this.result(edge.id,'expired');continue;}
      let accepted=false,lane,id;
      if(edge.kind==='fire') {
        lane='fire';id=this.fireSerial=Math.max(this.fireSerial,this.actor.fire.received)+1;
        accepted=this.world.fire(this.client,{...edge,id,button:edge.button==='fireSecondary'?1:0});
      } else {
        lane='actions';id=this.actionSerial=Math.max(this.actionSerial,this.actor.actions.received)+1;const actions=[],human=[];
        if(edge.kind==='action')actions.push(edge.action);
        else if(edge.kind==='spawn') {
          const className=this.registry.classes.get(edge.classId),post=edge.postId?this.registry.posts?.get(edge.postId):null;
          if(!className||edge.postId&&!post){this.result(edge.id,'rejected');continue;}
          human.push({kind:'spawn',className,post});
        } else if(['team','suicide','cancel'].includes(edge.kind))human.push({kind:edge.kind,...(edge.kind==='team'?{team:edge.team}:{})});
        else {this.result(edge.id,'rejected');continue;}
        // Delivery order may differ from the external edge's timestamp. This is
        // the next simulation tick, never a fabricated historical execution.
        accepted=this.world.actions(this.client,{epoch:this.epoch,generation:this.generation,id,tick,actions,human});
      }
      if(accepted)this.pending.set(`${lane}:${id}`,edge.id);else this.result(edge.id,'rejected');
    }
  }
  consumed() {
    for(const lane of ['actions','fire'])for(const outcome of this.actor[lane].drainOutcomes()) {
      const key=`${lane}:${outcome.id}`,id=this.pending.get(key);if(id===undefined)continue;
      this.pending.delete(key);this.result(id,outcome.status);
    }
  }
  acknowledge({epoch,generation,ids}) {if(epoch!==this.epoch||generation!==this.generation)return;for(const id of ids)this.results.delete(id);this.received=this.received.filter(id=>!ids.includes(id));}
  feedback() {return {epoch:this.epoch,generation:this.generation,received:this.received.slice(),results:[...this.results.values()].slice(0,32)};}
}
