-- Delivery journal for the transactional outbox.
--
-- outbox_events records that an email should be sent; outbox_deliveries records
-- whether it was actually handed to the SMTP server, and by which worker. The
-- journal is what makes replays after a crash idempotent: a delivery that was
-- journaled as sent (or that expired mid-flight) is never sent again, it is
-- only confirmed.
CREATE TABLE outbox_deliveries (
  event_id uuid PRIMARY KEY REFERENCES outbox_events(id) ON DELETE CASCADE,
  idempotency_key text NOT NULL UNIQUE,
  message_id text,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
  attempts integer NOT NULL DEFAULT 0,
  claimed_by text,
  claimed_at timestamptz,
  sent_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_deliveries_status_idx ON outbox_deliveries(status, claimed_at);

-- Backfill the journal from historical events. Already processed events are
-- recorded as sent so replays can never send them again. Events stuck in
-- processing keep a sending lease anchored at their last update, so the
-- recovery flow finalizes them without resending instead of duplicating mail.
INSERT INTO outbox_deliveries(event_id, idempotency_key, status, claimed_at, sent_at, attempts, created_at, updated_at)
SELECT id,
       'outbox:' || id::text,
       CASE status
         WHEN 'processed' THEN 'sent'
         WHEN 'failed' THEN 'failed'
         WHEN 'processing' THEN 'sending'
         ELSE 'pending'
       END,
       CASE WHEN status = 'processing' THEN updated_at END,
       processed_at,
       attempts,
       created_at,
       updated_at
FROM outbox_events;
