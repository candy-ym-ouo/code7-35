import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { config } from "./config";
import { pool } from "./db";
import { createSmtpMailer, messageIdDomain } from "./outbox/mailer";
import { deliverClaimedEvent } from "./outbox/machine";
import { createOutboxStore } from "./outbox/store";

// Identity of this worker instance. Leases are only meaningful if every
// instance claims events under a distinct owner id.
const ownerId = `worker-${hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;

const store = createOutboxStore(pool, { leaseSeconds: config.OUTBOX_LEASE_SECONDS });

const mailer = createSmtpMailer({
  host: config.SMTP_HOST,
  port: config.SMTP_PORT,
  secure: config.SMTP_SECURE,
  user: config.SMTP_USER,
  password: config.SMTP_PASSWORD,
  from: config.MAIL_FROM,
  timeoutMs: config.OUTBOX_SMTP_TIMEOUT_MS
});

const messageIdDomainValue = messageIdDomain(config.MAIL_FROM);

// Events are claimed one at a time with their own lease, so a slow SMTP send
// can never expire a shared batch lease while another instance is still working.
export async function dispatchOutbox(eventId?: string): Promise<void> {
  const limit = eventId ? 1 : config.OUTBOX_BATCH_SIZE;
  for (let dispatched = 0; dispatched < limit; dispatched += 1) {
    const event = await store.claimNext({ ownerId, eventId });
    if (!event) return;
    try {
      await deliverClaimedEvent(
        {
          store,
          mailer,
          maxAttempts: config.OUTBOX_MAX_ATTEMPTS,
          maxAmbiguous: config.OUTBOX_MAX_AMBIGUOUS,
          messageIdDomain: messageIdDomainValue,
          log: (name, details) => console.error({ ...details }, name)
        },
        event
      );
    } catch (error) {
      // The event keeps its lease and becomes replayable only after it expires;
      // the next claim reconciles the interrupted 'sending' attempt.
      console.error({ eventId: event.id, error }, "outbox delivery interrupted");
    }
  }
}
