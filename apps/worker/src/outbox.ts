import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import nodemailer from "nodemailer";
import { outboxIdempotencyKey, outboxMessageId } from "@map/shared/server";
import { config } from "./config";
import { pool } from "./db";
import {
  MAX_DELIVERY_ATTEMPTS,
  WAIT_RETRY_SECONDS,
  computeBackoffSeconds,
  planDelivery,
  type DeliveryState,
  type DeliveryStatus
} from "./outbox-plan";

const transporter = nodemailer.createTransport({
  host: config.SMTP_HOST,
  port: config.SMTP_PORT,
  secure: config.SMTP_SECURE,
  auth: config.SMTP_USER ? { user: config.SMTP_USER, pass: config.SMTP_PASSWORD } : undefined,
  // Bounded timeouts guarantee a live send can never outlive the delivery
  // lease, so an expired lease always means the previous owner is gone.
  connectionTimeout: 30_000,
  greetingTimeout: 30_000,
  socketTimeout: 60_000
});

// Unique per worker process; fences every delivery lease and confirmation.
const INSTANCE_ID = `${hostname()}:${process.pid}:${randomUUID()}`;

type OutboxEvent = {
  id: string;
  payload: { to: string; subject: string; text: string; html: string };
  attempts: number;
};

type DeliveryRow = {
  event_id: string;
  status: DeliveryStatus;
  attempts: number;
  claimed_by: string | null;
  claimed_at: Date | null;
  message_id: string | null;
  last_error: string | null;
};

export async function recoverStuckOutbox(): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
     SET status = 'pending', available_at = now(), last_error = 'Recovered after worker timeout', updated_at = now()
     WHERE status = 'processing' AND updated_at < now() - interval '10 minutes'`
  );
}

export async function dispatchOutbox(eventId?: string): Promise<void> {
  // Cross-instance claiming boundary (event level): the claim and the status
  // change are one statement, so two workers can never claim the same event.
  const result = await pool.query<OutboxEvent>(
    `WITH claimed AS (
       SELECT id FROM outbox_events
       WHERE status = 'pending'
         AND available_at <= now()
         AND ($1::uuid IS NULL OR id = $1::uuid)
       ORDER BY created_at
       FOR UPDATE SKIP LOCKED
       LIMIT 20
     )
     UPDATE outbox_events o
     SET status = 'processing', updated_at = now()
     FROM claimed
     WHERE o.id = claimed.id
     RETURNING o.id, o.payload, o.attempts`,
    [eventId ?? null]
  );

  for (const event of result.rows) {
    try {
      await deliverEvent(event);
    } catch (error) {
      // The event stays in processing and is recovered by lease expiry; the
      // journal decides on replay whether the mail may be sent again.
      console.error({ eventId: event.id, error }, "outbox delivery cycle failed");
    }
  }
}

async function deliverEvent(event: OutboxEvent): Promise<void> {
  const delivery = await loadDelivery(event.id);
  const plan = planDelivery(toState(delivery), new Date());
  switch (plan.action) {
    case "send":
      await sendAndConfirm(event, delivery);
      return;
    case "confirm":
      // Historical replay boundary: the mail was already handed to the MTA,
      // only the confirmation was lost. Confirm without sending again.
      await confirmEvent(event.id);
      return;
    case "finalize-stale":
      await finalizeStaleDelivery(event.id, delivery);
      return;
    case "wait":
      // A live worker owns the delivery lease; release the event and retry later.
      await pool.query(
        `UPDATE outbox_events
         SET status = 'pending',
             available_at = now() + ($2::text || ' seconds')::interval,
             last_error = 'Delivery in progress on another worker',
             updated_at = now()
         WHERE id = $1`,
        [event.id, String(WAIT_RETRY_SECONDS)]
      );
      return;
    case "dead":
      await pool.query(
        `UPDATE outbox_events SET status = 'failed', last_error = $2, updated_at = now() WHERE id = $1`,
        [event.id, delivery.last_error ?? "Delivery attempts exhausted"]
      );
      return;
  }
}

async function loadDelivery(eventId: string): Promise<DeliveryRow> {
  // Defensive: events created through the API always get a journal row in the
  // same transaction, and the migration backfilled historical rows.
  await pool.query(
    `INSERT INTO outbox_deliveries(event_id, idempotency_key)
     VALUES ($1, $2)
     ON CONFLICT (event_id) DO NOTHING`,
    [eventId, outboxIdempotencyKey(eventId)]
  );
  const result = await pool.query<DeliveryRow>(
    `SELECT event_id, status, attempts, claimed_by, claimed_at, message_id, last_error
     FROM outbox_deliveries WHERE event_id = $1`,
    [eventId]
  );
  return result.rows[0]!;
}

async function sendAndConfirm(event: OutboxEvent, delivery: DeliveryRow): Promise<void> {
  // Cross-instance claiming boundary (delivery level): atomically take the
  // lease. Only the instance that wins this compare-and-swap may send.
  const messageId = delivery.message_id ?? outboxMessageId(event.id, config.MAIL_FROM);
  const claimed = await pool.query(
    `UPDATE outbox_deliveries
     SET status = 'sending', claimed_by = $2, claimed_at = now(), message_id = $3, updated_at = now()
     WHERE event_id = $1 AND status = 'pending'`,
    [event.id, INSTANCE_ID, messageId]
  );
  if (claimed.rowCount === 0) {
    console.log(`skip outbox event ${event.id}: delivery lease held elsewhere`);
    return;
  }

  try {
    await transporter.sendMail({
      from: config.MAIL_FROM,
      to: event.payload.to,
      subject: event.payload.subject,
      text: event.payload.text,
      html: event.payload.html,
      // Deterministic Message-ID: replays of the same event always produce the
      // same identifier, so MTA/mailbox deduplication can suppress repeats.
      messageId
    });
  } catch (error) {
    await recordSendFailure(event, delivery, error);
    return;
  }

  // Confirm in one transaction: the journal records the handoff and the event
  // is completed atomically, so a crash can no longer hide a delivered mail.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const confirmed = await client.query(
      `UPDATE outbox_deliveries
       SET status = 'sent', sent_at = now(), last_error = NULL, updated_at = now()
       WHERE event_id = $1 AND status = 'sending' AND claimed_by = $2`,
      [event.id, INSTANCE_ID]
    );
    if (confirmed.rowCount === 0) {
      // Lost the lease while sending; another worker finalized the delivery.
      await client.query("ROLLBACK");
      return;
    }
    await client.query(
      `UPDATE outbox_events
       SET status = 'processed', processed_at = now(), last_error = NULL, updated_at = now()
       WHERE id = $1`,
      [event.id]
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function recordSendFailure(event: OutboxEvent, delivery: DeliveryRow, error: unknown): Promise<void> {
  const attempts = delivery.attempts + 1;
  const exhausted = attempts >= MAX_DELIVERY_ATTEMPTS;
  const message = error instanceof Error ? error.message.slice(0, 1000) : "Unknown mail error";
  // Only the lease holder may record the outcome of its own attempt.
  await pool.query(
    `UPDATE outbox_deliveries
     SET status = $2, attempts = $3, last_error = $4, updated_at = now()
     WHERE event_id = $1 AND status = 'sending' AND claimed_by = $5`,
    [event.id, exhausted ? "failed" : "pending", attempts, message, INSTANCE_ID]
  );
  await pool.query(
    `UPDATE outbox_events
     SET status = $2,
         attempts = $3,
         available_at = now() + ($4::text || ' seconds')::interval,
         last_error = $5,
         updated_at = now()
     WHERE id = $1`,
    [event.id, exhausted ? "failed" : "pending", attempts, String(computeBackoffSeconds(attempts)), message]
  );
}

async function confirmEvent(eventId: string): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
     SET status = 'processed', processed_at = now(), last_error = NULL, updated_at = now()
     WHERE id = $1`,
    [eventId]
  );
}

async function finalizeStaleDelivery(eventId: string, delivery: DeliveryRow): Promise<void> {
  // Historical replay boundary: the previous worker journaled the send and
  // then died before confirming. The mail may already be delivered, so the
  // replay must never send it again — finalize the journal and confirm.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `UPDATE outbox_deliveries
       SET status = 'sent', sent_at = now(),
           last_error = 'Finalized after worker crash before delivery confirmation',
           updated_at = now()
       WHERE event_id = $1 AND status = 'sending'`,
      [eventId]
    );
    await client.query(
      `UPDATE outbox_events
       SET status = 'processed', processed_at = now(),
           last_error = 'Recovered after worker crash before delivery confirmation',
           updated_at = now()
       WHERE id = $1`,
      [eventId]
    );
    await client.query(
      `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
       VALUES (NULL, 'outbox.delivery.finalized', 'outbox_event', $1, $2::jsonb)`,
      [
        eventId,
        JSON.stringify({
          claimedBy: delivery.claimed_by,
          claimedAt: delivery.claimed_at,
          finalizedBy: INSTANCE_ID
        })
      ]
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  console.warn({ eventId, claimedBy: delivery.claimed_by }, "finalized stale outbox delivery without resending");
}

function toState(delivery: DeliveryRow): DeliveryState {
  return {
    status: delivery.status,
    claimedBy: delivery.claimed_by,
    claimedAt: delivery.claimed_at,
    attempts: delivery.attempts
  };
}
