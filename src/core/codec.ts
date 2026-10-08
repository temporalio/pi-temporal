// Encrypts what goes into Temporal history. Prompts, final answers, and error text pass through
// Workflow and Activity payloads, and for a coding agent they often hold code or secrets. With a
// key set, the server stores only ciphertext. Off by default.
//
// Every client and Worker of a namespace must use the same key, or they can't read each other's
// payloads. To read them in the UI or CLI, run a codec server with this same codec.

import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import type { DataConverter, Payload, PayloadCodec } from "@temporalio/common";

const ENCODING = "binary/encrypted";
const CIPHER = "aes-256-gcm";
const IV_BYTES = 12;
const TAG_BYTES = 16;

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (data: Uint8Array) => new TextDecoder().decode(data);

/** AES-256-GCM over each whole payload. `keyId` names the key, so it can be rotated later. */
export class AesGcmCodec implements PayloadCodec {
  constructor(
    private readonly key: Buffer,
    private readonly keyId = "default",
  ) {
    if (key.length !== 32) throw new Error("the codec key must be 32 bytes");
  }

  async encode(payloads: Payload[]): Promise<Payload[]> {
    return payloads.map((payload) => {
      const iv = randomBytes(IV_BYTES);
      const cipher = createCipheriv(CIPHER, this.key, iv);
      // The whole payload, metadata included, so its encoding is hidden too.
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
      // Payloads written before a key was set stay readable.
      if (!payload.metadata?.encoding || text(payload.metadata.encoding) !== ENCODING) {
        return payload;
      }
      const keyId = payload.metadata["encryption-key-id"];
      if (keyId && text(keyId) !== this.keyId) {
        throw new Error(`payload was encrypted with key ${text(keyId)}, not ${this.keyId}`);
      }
      const sealed = Buffer.from(payload.data ?? []);
      const iv = sealed.subarray(0, IV_BYTES);
      const tag = sealed.subarray(sealed.length - TAG_BYTES);
      const decipher = createDecipheriv(CIPHER, this.key, iv);
      decipher.setAuthTag(tag);
      const plain = Buffer.concat([
        decipher.update(sealed.subarray(IV_BYTES, sealed.length - TAG_BYTES)),
        decipher.final(),
      ]);
      return fromJson(JSON.parse(plain.toString()));
    });
  }
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

/** The data converter for a key, or undefined to store payloads as plain JSON. */
export const dataConverterFor = (key: Buffer | undefined): DataConverter | undefined =>
  key ? { payloadCodecs: [new AesGcmCodec(key)] } : undefined;
