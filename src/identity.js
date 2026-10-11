import {
  createHash,
  generateKeyPairSync,
  sign,
  verify,
  createPublicKey,
  randomBytes,
} from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
export const keyId = (key) => createHash("sha256").update(key).digest("hex");
export function loadIdentity(file) {
  if (existsSync(file)) return JSON.parse(readFileSync(file, "utf8"));
  mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  const pair = generateKeyPairSync("ed25519"),
    keys = {
      publicKey: pair.publicKey
        .export({ format: "der", type: "spki" })
        .toString("base64"),
      privateKey: pair.privateKey.export({ format: "pem", type: "pkcs8" }),
    };
  writeFileSync(file, JSON.stringify(keys), { flag: "wx", mode: 0o600 });
  return keys;
}
export function headers(keys, method, path, body = "") {
  const time = String(Math.floor(Date.now() / 1000)),
    nonce = randomBytes(16).toString("hex");
  return {
    "x-bfe-notice": "2026-10-11",
    "x-bfe-key": keys.publicKey,
    "x-bfe-time": time,
    "x-bfe-nonce": nonce,
    "x-bfe-signature": sign(
      null,
      Buffer.from(
        `${method}\n${path}\n${time}\n${nonce}\n${createHash("sha256").update(body).digest("hex")}`,
      ),
      keys.privateKey,
    ).toString("base64"),
  };
}
export function proof(keys, transcript) {
  return {
    publicKey: keys.publicKey,
    signature: sign(null, Buffer.from(transcript), keys.privateKey).toString(
      "base64",
    ),
  };
}
export function verifyProof(p, transcript) {
  try {
    if (
      typeof p.publicKey !== "string" ||
      p.publicKey.length !== 60 ||
      typeof p.signature !== "string" ||
      p.signature.length !== 88
    )
      return false;
    const key = createPublicKey({
      key: Buffer.from(p.publicKey, "base64"),
      format: "der",
      type: "spki",
    });
    return (
      key.asymmetricKeyType === "ed25519" &&
      verify(
        null,
        Buffer.from(transcript),
        key,
        Buffer.from(p.signature, "base64"),
      )
    );
  } catch {
    return false;
  }
}
