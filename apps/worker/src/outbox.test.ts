import { describe, expect, it } from "vitest";
import {
  DELIVERY_LEASE_MS,
  MAX_DELIVERY_ATTEMPTS,
  computeBackoffSeconds,
  planDelivery,
  type DeliveryState
} from "./outbox-plan";

const NOW = new Date("2026-09-24T12:00:00Z");

function state(overrides: Partial<DeliveryState>): DeliveryState {
  return { status: "pending", claimedBy: null, claimedAt: null, attempts: 0, ...overrides };
}

describe("planDelivery", () => {
  it("sends a delivery that was never attempted", () => {
    expect(planDelivery(state({ status: "pending" }), NOW)).toEqual({ action: "send" });
  });

  it("resends a delivery whose previous attempt explicitly failed", () => {
    expect(planDelivery(state({ status: "pending", attempts: 2 }), NOW)).toEqual({ action: "send" });
  });

  it("never resends a journaled send, only confirms the event", () => {
    // Crash after the journal recorded the handoff but before the event was
    // confirmed: replay must confirm, not send a duplicate.
    expect(planDelivery(state({ status: "sent" }), NOW)).toEqual({ action: "confirm" });
  });

  it("finalizes an expired in-flight lease without resending", () => {
    // Crash between the SMTP handoff and the confirmation: the lease is stale,
    // the mail may already be delivered, so replay must not send it again.
    const claimedAt = new Date(NOW.getTime() - DELIVERY_LEASE_MS - 1);
    expect(planDelivery(state({ status: "sending", claimedBy: "dead-worker", claimedAt }), NOW))
      .toEqual({ action: "finalize-stale" });
  });

  it("treats a sending lease without timestamp as expired", () => {
    expect(planDelivery(state({ status: "sending", claimedAt: null }), NOW))
      .toEqual({ action: "finalize-stale" });
  });

  it("waits while a live worker holds the lease", () => {
    const claimedAt = new Date(NOW.getTime() - DELIVERY_LEASE_MS + 60_000);
    expect(planDelivery(state({ status: "sending", claimedBy: "other-worker", claimedAt }), NOW))
      .toEqual({ action: "wait" });
  });

  it("uses the exact lease boundary", () => {
    const boundary = new Date(NOW.getTime() - DELIVERY_LEASE_MS);
    expect(planDelivery(state({ status: "sending", claimedAt: boundary }), NOW))
      .toEqual({ action: "finalize-stale" });
    const justInside = new Date(NOW.getTime() - DELIVERY_LEASE_MS + 1);
    expect(planDelivery(state({ status: "sending", claimedAt: justInside }), NOW))
      .toEqual({ action: "wait" });
  });

  it("fails terminally after the attempt budget is exhausted", () => {
    expect(planDelivery(state({ status: "failed", attempts: MAX_DELIVERY_ATTEMPTS }), NOW))
      .toEqual({ action: "dead" });
    expect(planDelivery(state({ status: "pending", attempts: MAX_DELIVERY_ATTEMPTS }), NOW))
      .toEqual({ action: "dead" });
  });

  it("is recomputable: the same journaled state always yields the same plan", () => {
    const journaled = state({ status: "sending", claimedBy: "w1", claimedAt: new Date(0) });
    expect(planDelivery(journaled, NOW)).toEqual(planDelivery(journaled, NOW));
  });
});

describe("computeBackoffSeconds", () => {
  it("grows exponentially and is capped", () => {
    expect(computeBackoffSeconds(1)).toBe(2);
    expect(computeBackoffSeconds(2)).toBe(4);
    expect(computeBackoffSeconds(4)).toBe(16);
    expect(computeBackoffSeconds(9)).toBe(300);
    expect(computeBackoffSeconds(20)).toBe(300);
  });
});
