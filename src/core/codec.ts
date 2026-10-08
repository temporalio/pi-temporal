// Coding agent payloads can contain code and secrets. With a key set, this codec encrypts
// payloads before the server stores them in history. Encryption is off by default.
//
// Clients and Workers that read each other's payloads must share the key. Reading encrypted
// payloads in the UI or CLI also needs a codec server with this codec.
//
// To rotate, make the new key current and keep the old one as a decrypt-only key. Keep it while a
// run that started before the rotation is open, then for the retention period after the last one
// closes, and for as long as archived histories are kept.

import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import type { DataConverter, Payload, PayloadCodec } from "@temporalio/common";

const ENCODING = "binary/encrypted";
const CIPHER = "aes-256-gcm";
const IV_BYTES = 12;
const TAG_BYTES = 16;

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (data: Uint8Array) => new TextDecoder().decode(data);

// Names a key without showing it. A payload carries the name of the key that sealed it, so a
// reader with several keys knows which one to use.
const keyIdOf = (key: Buffer) => createHash("sha256").update(key).digest("hex").slice(0, 16);

/**
 * AES-256-GCM over each whole payload. `key` encrypts. `oldKeys` only decrypt, so payloads sealed
 * before a rotation stay readable.
 */
export class AesGcmCodec implements PayloadCodec {
  private readonly keyId: string;
  private readonly keys: Map<string, Buffer>;

  constructor(
    private readonly key: Buffer,
    oldKeys: readonly Buffer[] = [],
  ) {
    for (const each of [key, ...oldKeys]) {
      if (each.length !== 32) throw new Error("a codec key must be 32 bytes");
    }
    this.keyId = keyIdOf(key);
    this.keys = new Map([...oldKeys, key].map((each) => [keyIdOf(each), each]));
  }

  async encode(payloads: Payload[]): Promise<Payload[]> {
    return payloads.map((payload) => {
      const iv = randomBytes(IV_BYTES);
      const cipher = createCipheriv(CIPHER, this.key, iv);
      // Encrypting metadata also hides the payload's encoding.
      const plain = Buffer.from(JSON.stringify(toJson(payload)));
      const sealed = Buffer.concat([iv, cipher.update(plain), cipher.final(), cipher.getAuthTag()]);
      return {
        metadata: { encoding: bytes(ENCODING), "encryption-key-id": bytes(this.keyId) },
        data: sealed,
      };
    });
  }

  async decode(payloads: Payload[]): Promise<Payload[]> {
    return payloads.map((payload) => {
      // Payloads written before a key was set stay readable. So would one written by anything that
      // can write history directly, which this codec doesn't guard against.
      if (!payload.metadata?.encoding || text(payload.metadata.encoding) !== ENCODING) {
        return payload;
      }
      const sealed = Buffer.from(payload.data ?? []);
      // Node accepts shorter tags, but this format requires a full tag.
      if (sealed.length < IV_BYTES + TAG_BYTES) throw new Error("encrypted payload is cut short");
      const named = payload.metadata["encryption-key-id"];
      const keyId = named ? text(named) : undefined;
      const known = keyId === undefined ? undefined : this.keys.get(keyId);
      if (known) return fromJson(JSON.parse(open(sealed, known).toString()));
      // A name no key has, such as the `default` of payloads sealed before keys were named. Each
      // key is tried, and the auth tag refuses every wrong one.
      for (const each of this.keys.values()) {
        try {
          return fromJson(JSON.parse(open(sealed, each).toString()));
        } catch {
          // The next key.
        }
      }
      throw new Error(`no codec key opens a payload sealed with key ${keyId ?? "(unnamed)"}`);
    });
  }
}

function open(sealed: Buffer, key: Buffer): Buffer {
  const iv = sealed.subarray(0, IV_BYTES);
  const tag = sealed.subarray(sealed.length - TAG_BYTES);
  const decipher = createDecipheriv(CIPHER, key, iv, { authTagLength: TAG_BYTES });
  decipher.setAuthTag(tag);
  return Buffer.concat([
    decipher.update(sealed.subarray(IV_BYTES, sealed.length - TAG_BYTES)),
    decipher.final(),
  ]);
}

type JsonPayload = { metadata: Record<string, string>; data: string };

const toJson = (payload: Payload): JsonPayload => ({
  metadata: Object.fromEntries(
    Object.entries(payload.metadata ?? {}).map(([k, v]) => [k, Buffer.from(v).toString("base64")]),
  ),
  data: Buffer.from(payload.data ?? []).toString("base64"),
});

const fromJson = (json: JsonPayload): Payload => ({
  metadata: Object.fromEntries(
    Object.entries(json.metadata).map(([k, v]) => [k, Buffer.from(v, "base64")]),
  ),
  data: Buffer.from(json.data, "base64"),
});

/** Moves failure messages into payloads, so the codec encrypts them too. */
export const FAILURE_CONVERTER_PATH = createRequire(import.meta.url).resolve(
  "./failure-converter.ts",
);

/** The data converter for the configured keys, or undefined to store payloads as plain JSON. */
export const dataConverterFor = (keys: {
  readonly codecKey?: Buffer;
  readonly codecOldKeys?: readonly Buffer[];
}): DataConverter | undefined =>
  keys.codecKey
    ? {
        payloadCodecs: [new AesGcmCodec(keys.codecKey, keys.codecOldKeys)],
        failureConverterPath: FAILURE_CONVERTER_PATH,
      }
    : undefined;
