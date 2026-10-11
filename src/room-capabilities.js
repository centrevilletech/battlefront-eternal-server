// A shared snapshot may use linear projectile events only while every active
// renderer understands them. Each transition begins a fresh delta chain.
export function updateRoomCapabilities(world,clients) {
 if(typeof world.setLinearProjectiles!=='function')return;
 const active=[...clients.values()].filter(state=>state.started);
 const enabled=active.length>0&&active.every(state=>state.linearProjectiles===true);
 if(enabled===world.linearProjectiles)return;
 world.setLinearProjectiles(enabled);
 for(const state of active)state.keyframe=true;
}
