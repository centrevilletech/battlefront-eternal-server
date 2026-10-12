// Operator-only diagnostic controls. Unset values preserve production defaults.
export function readTransportTuning(env = process.env) {
  const integer = (name, min, max, fallback) => {
    const value = env[name];
    if (value === undefined || value === '') return fallback;
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) < min || Number(value) > max)
      throw Error(`${name} must be an integer between ${min} and ${max}`);
    return Number(value);
  };
  return {
    sctpSendBuffer: integer('BFE_EXPERIMENTAL_SCTP_SEND_BUFFER', 32768, 1048576, 0),
    stateBufferLimit: integer('BFE_EXPERIMENTAL_STATE_BUFFER', 1024, 65536, 65536),
    journalFrameBytes: integer('BFE_EXPERIMENTAL_JOURNAL_FRAME', 512, 20000, 20000),
  };
}

export const transportTuning = readTransportTuning();
const configured = new WeakSet();
export function configureNativeTransport(native, tuning = transportTuning) {
  if (!tuning.sctpSendBuffer || configured.has(native)) return;
  native.setSctpSettings({ sendBufferSize: tuning.sctpSendBuffer });
  configured.add(native);
}
