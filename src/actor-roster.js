// Operator-only measurement census. Read authoritative objects, not the set of
// records the transport happened to capture or deliver: omitted actors matter.
export function liveActorRoster(game) {
  const soldiers = game.units.all().filter(unit => unit.soldier && unit.combat && unit.alive && !unit.removed)
    .map(unit => ({kind: 'soldier', id: unit.id, generation: unit.generation}));
  const vehicles = game.vehicles.vehicles.filter(vehicle => vehicle.alive)
    .map(vehicle => ({kind: 'vehicle', id: vehicle.aiTarget.id, generation: vehicle.aiTarget.generation}));
  return {tick: game.worldClock.tick, actors: [...soldiers, ...vehicles]};
}
