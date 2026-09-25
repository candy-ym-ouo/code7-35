import { z } from "zod";
import { classifySmtpError } from "./classify";
import type { Mailer } from "./mailer";
import type { ClaimedEvent, OutboxStore } from "./store";

const emailPayloadSchema = z.object({
  to: z.string().min(1),
  subject: z.string(),
  text: z.string(),
  html: z.string()
});

export type DeliveryResult = "sent" | "retry" | "parked" | "fence-lost";

export type MachineDeps = {
  store: OutboxStore;
  mailer: Mailer;
  maxAttempts: number;
  maxAmbiguous: number;
  messageIdDomain: string;
  log?: ((event: string, details: Record<string, unknown>) => void) | undefined;
};

export function backoffSeconds(attempts: number): number {
  return Math.min(300, 2 ** attempts);
}

export function deliveryMessageId(deliveryKey: string, domain: string): string {
  return `<${deliveryKey}@${domain}>`;
}

export async function deliverClaimedEvent(deps: MachineDeps, event: ClaimedEvent): Promise<DeliveryResult> {
  const parsed = emailPayloadSchema.safeParse(event.payload);
  if (!parsed.success) {
    await deps.store.park(event, "Outbox payload is not a deliverable email");
    deps.log?.("outbox.parked", { eventId: event.id, reason: "invalid_payload" });
    return "parked";
  }
  const payload = parsed.data;

  // Historical replay boundary: fold any attempt a previous owner left in
  // 'sending' into the ambiguous budget before any new physical send. The
  // lease guarantees such attempts have actually terminated (SMTP timeout <
  // lease length), only their outcome is unknown.
  const ambiguousCount = await deps.store.reconcileSending(event);
  if (ambiguousCount === null) {
    deps.log?.("outbox.fence_lost", { eventId: event.id, stage: "reconcile" });
    return "fence-lost";
  }
  if (ambiguousCount > deps.maxAmbiguous) {
    await deps.store.park(event, "Ambiguous delivery outcomes exceeded the replay budget");
    deps.log?.("outbox.parked", { eventId: event.id, reason: "ambiguous_budget", ambiguousCount });
    return "parked";
  }
  const current: ClaimedEvent = { ...event, ambiguousCount };

  // Durable in-flight record before the network call. A crash from here on is
  // observable as a leftover 'sending' row on the next replay.
  const messageId = deliveryMessageId(current.deliveryKey, deps.messageIdDomain);
  const attempt = await deps.store.recordSending(current, { messageId });
  if (attempt === null) {
    deps.log?.("outbox.fence_lost", { eventId: current.id, stage: "record" });
    return "fence-lost";
  }

  let outcome: { kind: "sent" } | { kind: "failed" | "ambiguous"; error: string };
  try {
    await deps.mailer.send({
      to: payload.to,
      subject: payload.subject,
      text: payload.text,
      html: payload.html,
      messageId,
      headers: {
        "X-Outbox-Event-Id": current.id,
        "X-Outbox-Delivery-Key": current.deliveryKey
      }
    });
    outcome = { kind: "sent" };
  } catch (error) {
    outcome = {
      kind: classifySmtpError(error),
      error: error instanceof Error ? error.message.slice(0, 1000) : "Unknown mail error"
    };
  }

  if (outcome.kind === "sent") {
    const confirmed = await deps.store.markSent(current, { attemptId: attempt.attemptId });
    if (!confirmed) {
      deps.log?.("outbox.fence_lost", { eventId: current.id, stage: "confirm" });
      return "fence-lost";
    }
    deps.log?.("outbox.sent", { eventId: current.id, attemptNo: attempt.attemptNo, messageId });
    return "sent";
  }

  const attempts = current.attempts + 1;
  const ambiguousTotal = outcome.kind === "ambiguous" ? current.ambiguousCount + 1 : current.ambiguousCount;
  const terminal = attempts >= deps.maxAttempts || ambiguousTotal > deps.maxAmbiguous;
  const recorded = await deps.store.markFailure(current, {
    attemptId: attempt.attemptId,
    kind: outcome.kind,
    error: outcome.error,
    attempts,
    ambiguousCount: ambiguousTotal,
    terminal,
    retryAfterSeconds: backoffSeconds(attempts)
  });
  if (!recorded) {
    deps.log?.("outbox.fence_lost", { eventId: current.id, stage: "fail" });
    return "fence-lost";
  }
  deps.log?.(terminal ? "outbox.parked" : "outbox.retry", {
    eventId: current.id,
    attemptNo: attempt.attemptNo,
    kind: outcome.kind,
    error: outcome.error
  });
  return terminal ? "parked" : "retry";
}
