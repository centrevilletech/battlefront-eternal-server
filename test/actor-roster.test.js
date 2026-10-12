import test from 'node:test';
import assert from 'node:assert/strict';
import {liveActorRoster} from '../src/actor-roster.js';

test('measurement roster includes unsupported live actors and excludes dead or removed lives', () => {
  const unit = (id, extra = {}) => ({id, generation: 2, alive: true, soldier: {}, combat: {}, position: {toArray: () => [id, 0, 0]}, ...extra});
  const game = {worldClock: {tick: 400}, units: {all: () => [unit(1), unit(2, {walker: true}), unit(3, {alive: false}), unit(4, {removed: true})]},
    vehicles: {vehicles: [{alive: true, aiTarget: {id: 10, generation: 5}}, {alive: false, aiTarget: {id: 11, generation: 1}}]}};
  const result = liveActorRoster(game);
  assert.deepEqual(result, {tick: 400, actors: [{kind: 'soldier', id: 1, generation: 2, position: [1, 0, 0]}, {kind: 'soldier', id: 2, generation: 2, position: [2, 0, 0]}, {kind: 'vehicle', id: 10, generation: 5, position: null}]});
  result.actors[2].generation = 100;
  assert.equal(game.vehicles.vehicles[0].aiTarget.generation, 5);
});
