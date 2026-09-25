import dotenv from "dotenv";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

dotenv.config({ path: process.env.ENV_FILE || join(dirname(fileURLToPath(import.meta.url)), "../../../.env") });
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379/0"),
  S3_ENDPOINT: z.string().url(),
  S3_PUBLIC_ENDPOINT: z.string().url(),
  S3_REGION: z.string().default("us-east-1"),
  S3_ACCESS_KEY: z.string().min(1),
  S3_SECRET_KEY: z.string().min(1),
  S3_QUARANTINE_BUCKET: z.string().min(1),
  S3_PUBLIC_BUCKET: z.string().min(1),
  MEDIA_MAX_PIXELS: z.coerce.number().int().positive().default(20_000_000),
  PRIVACY_DETECTOR_URL: z.string().url().optional().or(z.literal("")),
  PRIVACY_BLUR_SIGMA: z.coerce.number().positive().default(32),
  PRIVACY_BLUR_PADDING: z.coerce.number().min(0).max(0.5).default(0.08),
  ORIGINAL_RETENTION_HOURS: z.coerce.number().positive().default(24),
  CLAMAV_ENABLED: z.string().default("true").transform((value) => value === "true"),
  CLAMAV_HOST: z.string().default("localhost"),
  CLAMAV_PORT: z.coerce.number().int().positive().default(3310),
  SMTP_HOST: z.string().default("localhost"),
  SMTP_PORT: z.coerce.number().int().positive().default(1025),
  SMTP_SECURE: z.string().default("false").transform((value) => value === "true"),
  SMTP_USER: z.string().optional(),
  SMTP_PASSWORD: z.string().optional(),
  MAIL_FROM: z.string().default("公共空间细节地图 <noreply@example.test>"),
  // A lease must outlive any single SMTP send. Otherwise a send still in flight
  // could be reconciled as ambiguous and delivered again by another instance.
  OUTBOX_LEASE_SECONDS: z.coerce.number().int().positive().default(120),
  OUTBOX_SMTP_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),
  OUTBOX_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  // Outcomes that do not prove non-delivery (mid-session timeout/disconnect,
  // crashed worker) may be replayed at most this many times with the same
  // delivery identity, after which the event is parked for reconciliation.
  OUTBOX_MAX_AMBIGUOUS: z.coerce.number().int().min(0).default(2),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(20)
}).refine(
  (value) => value.OUTBOX_LEASE_SECONDS * 1000 > value.OUTBOX_SMTP_TIMEOUT_MS,
  { message: "OUTBOX_LEASE_SECONDS must exceed OUTBOX_SMTP_TIMEOUT_MS / 1000" }
);

export const config = envSchema.parse(process.env);
