import test from 'node:test';
import assert from 'node:assert/strict';
import { readTransportTuning, configureNativeTransport } from '../src/transport-tuning.js';

test('experimental transport tuning preserves defaults and rejects unsafe bounds', () => {
  assert.deepEqual(readTransportTuning({}), { sctpSendBuffer: 0, stateBufferLimit: 65536, journalFrameBytes: 20000 });
  assert.deepEqual(readTransportTuning({ BFE_EXPERIMENTAL_SCTP_SEND_BUFFER:'32768', BFE_EXPERIMENTAL_STATE_BUFFER:'4096', BFE_EXPERIMENTAL_JOURNAL_FRAME:'1000' }), { sctpSendBuffer:32768, stateBufferLimit:4096, journalFrameBytes:1000 });
  for (const [name, values] of Object.entries({ BFE_EXPERIMENTAL_SCTP_SEND_BUFFER:['0','32767','1048577'], BFE_EXPERIMENTAL_STATE_BUFFER:['0','65537'], BFE_EXPERIMENTAL_JOURNAL_FRAME:['511','20001'] }))
    for (const value of [...values,'-1','NaN','1.5',' 4096']) assert.throws(() => readTransportTuning({ [name]:value }), /must be an integer/);
});

test('native global SCTP settings change only once and only on explicit opt-in', () => {
  const calls = [], native = { setSctpSettings: value => calls.push(value) };
  configureNativeTransport(native, readTransportTuning({}));
  assert.deepEqual(calls, []);
  const tuning = readTransportTuning({ BFE_EXPERIMENTAL_SCTP_SEND_BUFFER:'32768' });
  configureNativeTransport(native, tuning); configureNativeTransport(native, tuning);
  assert.deepEqual(calls, [{ sendBufferSize:32768 }]);
});
