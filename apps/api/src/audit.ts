import type { PoolClient } from "pg";
import { outboxIdempotencyKey } from "@map/shared/server";

export async function recordAudit(
  client: PoolClient,
  input: {
    actorId?: string | null;
    action: string;
    resourceType: string;
    resourceId?: string | null;
    metadata?: Record<string, unknown>;
  }
): Promise<void> {
  await client.query(
    `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
     VALUES ($1, $2, $3, $4, $5::jsonb)`,
    [
      input.actorId ?? null,
      input.action,
      input.resourceType,
      input.resourceId ?? null,
      JSON.stringify(input.metadata ?? {})
    ]
  );
}

export async function queueOutbox(
  client: PoolClient,
  input: {
    eventType: string;
    aggregateType: string;
    aggregateId: string;
    payload: Record<string, unknown>;
  }
): Promise<string> {
  const result = await client.query<{ id: string }>(
    `INSERT INTO outbox_events(event_type, aggregate_type, aggregate_id, payload)
     VALUES ($1, $2, $3, $4::jsonb)
     RETURNING id`,
    [input.eventType, input.aggregateType, input.aggregateId, JSON.stringify(input.payload)]
  );
  const eventId = result.rows[0]!.id;
  // The delivery journal is created in the same transaction. It is the durable
  // idempotency record the worker uses to avoid duplicate sends after crashes.
  await client.query(
    `INSERT INTO outbox_deliveries(event_id, idempotency_key)
     VALUES ($1, $2)`,
    [eventId, outboxIdempotencyKey(eventId)]
  );
  return eventId;
}

export async function createNotification(
  client: PoolClient,
  input: {
    userId: string;
    type: string;
    title: string;
    body: string;
    link?: string | null;
  }
): Promise<string> {
  const result = await client.query<{ id: string }>(
    `INSERT INTO notifications(user_id, type, title, body, link)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING id`,
    [input.userId, input.type, input.title, input.body, input.link ?? null]
  );
  return result.rows[0]!.id;
}
