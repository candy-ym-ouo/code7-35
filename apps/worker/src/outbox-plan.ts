/**
 * Pure, recomputable delivery planning for the outbox journal.
 *
 * The delivery journal (outbox_deliveries) is the durable record of whether an
 * email was handed to the SMTP server. Every replay decision is a pure function
 * of the journaled state, so crashing and re-running the dispatcher always
 * converges to the same outcome instead of repeating the side effect.
 *
 * Replay boundary:
 * - `sent`     -> the mail was handed off; only confirm the event, never resend.
 * - `sending`  -> a worker holds the delivery lease. If the lease is expired the
 *                 owner died before confirming, so the delivery is finalized as
 *                 sent without resending (a resend could duplicate the mail).
 * - `pending`  -> no completed send is journaled; sending is safe.
 * - `failed`   -> terminally failed; fail the event.
 */

export const DELIVERY_LEASE_MS = 10 * 60 * 1000;
export const MAX_DELIVERY_ATTEMPTS = 5;
export const WAIT_RETRY_SECONDS = 30;

export type DeliveryStatus = "pending" | "sending" | "sent" | "failed";

export interface DeliveryState {
  status: DeliveryStatus;
  claimedBy: string | null;
  claimedAt: Date | null;
  attempts: number;
}

export type DeliveryPlan =
  | { action: "send" }
  | { action: "confirm" }
  | { action: "finalize-stale" }
  | { action: "wait" }
  | { action: "dead" };

export function planDelivery(delivery: DeliveryState, now: Date): DeliveryPlan {
  if (delivery.status === "sent") return { action: "confirm" };
  if (delivery.status === "failed") return { action: "dead" };
  if (delivery.attempts >= MAX_DELIVERY_ATTEMPTS) return { action: "dead" };
  if (delivery.status === "pending") return { action: "send" };
  // status === "sending": a missing claim timestamp is treated as expired so
  // the replay never resends a delivery whose outcome is unknown.
  const claimedAt = delivery.claimedAt?.getTime() ?? 0;
  return now.getTime() - claimedAt >= DELIVERY_LEASE_MS
    ? { action: "finalize-stale" }
    : { action: "wait" };
}

export function computeBackoffSeconds(attempts: number): number {
  return Math.min(300, 2 ** Math.max(1, attempts));
}
