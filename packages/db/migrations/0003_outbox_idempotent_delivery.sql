-- Idempotent outbox delivery.
--
-- Cross-instance claim boundary: an event may only be delivered by the worker
-- instance that holds a live lease on it (claimed_by + claim_token +
-- lease_expires_at). Every later state transition is fenced by claim_token, so
-- a worker whose lease expired can no longer confirm or fail the event.
--
-- Historical replay boundary: only events that provably never reached the SMTP
-- server may be replayed freely. Events with an unknown outcome (the worker
-- crashed or the connection dropped after the server may have accepted the
-- mail) are replayed with the same stable identity (delivery_key, rendered as
-- a deterministic Message-ID), a bounded number of times, and are then parked
-- as failed for manual reconciliation against the delivery ledger.

ALTER TABLE outbox_events
  ADD COLUMN delivery_key uuid NOT NULL DEFAULT gen_random_uuid(),
  ADD COLUMN claimed_by text,
  ADD COLUMN claim_token uuid,
  ADD COLUMN lease_expires_at timestamptz,
  ADD COLUMN ambiguous_count integer NOT NULL DEFAULT 0;

ALTER TABLE outbox_events
  ADD CONSTRAINT outbox_events_delivery_key_key UNIQUE (delivery_key);

DROP INDEX IF EXISTS outbox_events_queue_idx;
CREATE INDEX outbox_events_pending_idx ON outbox_events(available_at) WHERE status = 'pending';
CREATE INDEX outbox_events_lease_idx ON outbox_events(lease_expires_at) WHERE status = 'processing';

CREATE TABLE outbox_delivery_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id uuid NOT NULL REFERENCES outbox_events(id) ON DELETE CASCADE,
  attempt_no integer NOT NULL,
  message_id text NOT NULL,
  status text NOT NULL DEFAULT 'sending' CHECK (status IN ('sending', 'sent', 'failed', 'ambiguous')),
  error text,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (event_id, attempt_no)
);
CREATE INDEX outbox_delivery_attempts_event_idx ON outbox_delivery_attempts(event_id, attempt_no);
CREATE INDEX outbox_delivery_attempts_message_idx ON outbox_delivery_attempts(message_id);

-- Migration runs in the deploy window before any new worker starts, so any
-- 'processing' row is a zombie from the old crash-before-confirm flow. Return
-- it to the queue so the new lease-bounded claim picks it up instead of
-- leaving it stranded with a NULL lease that nothing can ever reclaim.
UPDATE outbox_events
SET status = 'pending', available_at = now(), updated_at = now()
WHERE status = 'processing';

