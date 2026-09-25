import { randomUUID } from "node:crypto";
import type { Pool } from "pg";

export type ClaimedEvent = {
  id: string;
  // Stable identity reused on every replay of this event.
  deliveryKey: string;
  // Fencing token: every later write must prove it still holds this token.
  claimToken: string;
  payload: unknown;
  attempts: number;
  ambiguousCount: number;
};

export type FailureKind = "failed" | "ambiguous";

export interface OutboxStore {
  // Atomically claims the next deliverable event:
  //   pending + available_at reached   (never attempted / scheduled retry)
  //   processing + lease expired       (previous owner crashed or hung)
  // FOR UPDATE SKIP LOCKED makes this safe across worker instances.
  claimNext(input: { ownerId: string; eventId?: string | undefined }): Promise<ClaimedEvent | null>;
  // Marks attempts a previous owner left in 'sending' as ambiguous and folds
  // them into ambiguous_count. Returns the new count, or null if the fence was
  // lost (another instance already owns the event).
  reconcileSending(event: ClaimedEvent): Promise<number | null>;
  // Durably records an in-flight physical send before talking to SMTP.
  recordSending(event: ClaimedEvent, input: { messageId: string }): Promise<{ attemptId: string; attemptNo: number } | null>;
  markSent(event: ClaimedEvent, input: { attemptId: string }): Promise<boolean>;
  markFailure(
    event: ClaimedEvent,
    input: {
      attemptId: string;
      kind: FailureKind;
      error: string;
      attempts: number;
      ambiguousCount: number;
      terminal: boolean;
      retryAfterSeconds: number;
    }
  ): Promise<boolean>;
  // Parks an event without adding an attempt (e.g. invalid payload, ambiguous
  // budget already exhausted before any new send).
  park(event: ClaimedEvent, error: string): Promise<boolean>;
}

type ClaimedRow = {
  id: string;
  delivery_key: string;
  claim_token: string;
  payload: unknown;
  attempts: number;
  ambiguous_count: number;
};

export const CLAIM_NEXT_SQL = `
  WITH candidate AS (
    SELECT id
    FROM outbox_events
    WHERE (
        (status = 'pending' AND available_at <= now())
        OR (status = 'processing' AND lease_expires_at IS NOT NULL AND lease_expires_at < now())
      )
      AND ($3::uuid IS NULL OR id = $3::uuid)
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  )
  UPDATE outbox_events AS o
  SET status = 'processing',
      claimed_by = $1,
      claim_token = $2,
      lease_expires_at = now() + make_interval(secs => $4),
      updated_at = now()
  FROM candidate
  WHERE o.id = candidate.id
  RETURNING o.id, o.delivery_key, o.claim_token, o.payload, o.attempts, o.ambiguous_count
`;

export const RECONCILE_SENDING_SQL = `
  WITH stale AS (
    UPDATE outbox_delivery_attempts AS a
    SET status = 'ambiguous',
        error = 'Previous delivery attempt ended without a recorded outcome',
        completed_at = now()
    WHERE a.event_id = $1
      AND a.status = 'sending'
      AND EXISTS (
        SELECT 1 FROM outbox_events AS o
        WHERE o.id = a.event_id AND o.claim_token = $2
      )
    RETURNING a.id
  )
  UPDATE outbox_events AS o
  SET ambiguous_count = o.ambiguous_count + (SELECT count(*) FROM stale),
      updated_at = now()
  WHERE o.id = $1 AND o.claim_token = $2
  RETURNING o.ambiguous_count
`;

export const RECORD_SENDING_SQL = `
  INSERT INTO outbox_delivery_attempts(event_id, attempt_no, message_id, status)
  SELECT $1,
         (SELECT COALESCE(MAX(a.attempt_no), 0) + 1
          FROM outbox_delivery_attempts AS a
          WHERE a.event_id = $1),
         $3,
         'sending'
  WHERE EXISTS (
    SELECT 1 FROM outbox_events AS o
    WHERE o.id = $1 AND o.claim_token = $2
  )
  RETURNING id, attempt_no
`;

export const MARK_SENT_SQL = `
  WITH attempt AS (
    UPDATE outbox_delivery_attempts AS a
    SET status = 'sent', completed_at = now()
    WHERE a.id = $3
      AND a.event_id = $1
      AND EXISTS (
        SELECT 1 FROM outbox_events AS o
        WHERE o.id = $1 AND o.claim_token = $2
      )
    RETURNING a.id
  )
  UPDATE outbox_events AS o
  SET status = 'processed',
      processed_at = now(),
      last_error = NULL,
      lease_expires_at = NULL,
      updated_at = now()
  WHERE o.id = $1
    AND o.claim_token = $2
    AND EXISTS (SELECT 1 FROM attempt)
`;

export const MARK_FAILURE_SQL = `
  WITH attempt AS (
    UPDATE outbox_delivery_attempts AS a
    SET status = $3, error = $4, completed_at = now()
    WHERE a.id = $5
      AND a.event_id = $1
      AND EXISTS (
        SELECT 1 FROM outbox_events AS o
        WHERE o.id = $1 AND o.claim_token = $2
      )
    RETURNING a.id
  )
  UPDATE outbox_events AS o
  SET status = $6,
      attempts = $7,
      ambiguous_count = $8,
      available_at = CASE
        WHEN $6 = 'pending' THEN now() + make_interval(secs => $9)
        ELSE o.available_at
      END,
      last_error = $4,
      lease_expires_at = NULL,
      updated_at = now()
  WHERE o.id = $1
    AND o.claim_token = $2
    AND EXISTS (SELECT 1 FROM attempt)
`;

export const PARK_SQL = `
  UPDATE outbox_events
  SET status = 'failed',
      last_error = $3,
      lease_expires_at = NULL,
      updated_at = now()
  WHERE id = $1 AND claim_token = $2
`;

export function createOutboxStore(pool: Pool, options: { leaseSeconds: number }): OutboxStore {
  const { leaseSeconds } = options;

  return {
    async claimNext({ ownerId, eventId }) {
      const claimToken = randomUUID();
      const result = await pool.query<ClaimedRow>(CLAIM_NEXT_SQL, [
        ownerId,
        claimToken,
        eventId ?? null,
        leaseSeconds
      ]);
      const row = result.rows[0];
      if (!row) return null;
      return {
        id: row.id,
        deliveryKey: row.delivery_key,
        claimToken: row.claim_token,
        payload: row.payload,
        attempts: row.attempts,
        ambiguousCount: row.ambiguous_count
      };
    },

    async reconcileSending(event) {
      const result = await pool.query<{ ambiguous_count: number }>(RECONCILE_SENDING_SQL, [
        event.id,
        event.claimToken
      ]);
      const row = result.rows[0];
      return row ? row.ambiguous_count : null;
    },

    async recordSending(event, { messageId }) {
      const result = await pool.query<{ id: string; attempt_no: number }>(RECORD_SENDING_SQL, [
        event.id,
        event.claimToken,
        messageId
      ]);
      const row = result.rows[0];
      return row ? { attemptId: row.id, attemptNo: row.attempt_no } : null;
    },

    async markSent(event, { attemptId }) {
      const result = await pool.query(MARK_SENT_SQL, [event.id, event.claimToken, attemptId]);
      return result.rowCount === 1;
    },

    async markFailure(event, input) {
      const result = await pool.query(MARK_FAILURE_SQL, [
        event.id,
        event.claimToken,
        input.kind,
        input.error,
        input.attemptId,
        input.terminal ? "failed" : "pending",
        input.attempts,
        input.ambiguousCount,
        input.retryAfterSeconds
      ]);
      return result.rowCount === 1;
    },

    async park(event, error) {
      const result = await pool.query(PARK_SQL, [event.id, event.claimToken, error]);
      return result.rowCount === 1;
    }
  };
}
