import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { hash, verify } from "@node-rs/argon2";

export async function hashPassword(password: string): Promise<string> {
  return hash(password, {
    algorithm: 2,
    memoryCost: 19456,
    timeCost: 2,
    parallelism: 1
  });
}

export async function verifyPassword(password: string, passwordHash: string): Promise<boolean> {
  try {
    return await verify(passwordHash, password);
  } catch {
    return false;
  }
}

export function randomToken(bytes = 32): string {
  return randomBytes(bytes).toString("base64url");
}

export function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function safeEqual(a: string, b: string): boolean {
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

/**
 * Deterministic idempotency key for an outbox event. Every replay recomputes
 * the same key from the event id, so the delivery journal can deduplicate
 * sends across worker crashes and instances.
 */
export function outboxIdempotencyKey(eventId: string): string {
  return `outbox:${eventId}`;
}

/**
 * Deterministic SMTP Message-ID for an outbox event. Replays of the same event
 * always produce the same identifier, so MTA/mailbox-level deduplication can
 * suppress repeats. The domain is derived from the configured MAIL_FROM.
 */
export function outboxMessageId(eventId: string, mailFrom: string): string {
  const match = mailFrom.match(/@([A-Za-z0-9.-]+)\s*>?\s*$/);
  const domain = match?.[1] ?? "localhost";
  return `<outbox.${eventId}@${domain}>`;
}

export function parseInteger(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}
