import { describe, expect, it } from "vitest";
import { outboxIdempotencyKey, outboxMessageId } from "./server";

describe("outboxIdempotencyKey", () => {
  it("is a deterministic function of the event id", () => {
    const eventId = "6f4b2c8e-5f6a-4c0a-9c3a-2f0d9c8b7a65";
    expect(outboxIdempotencyKey(eventId)).toBe(`outbox:${eventId}`);
    expect(outboxIdempotencyKey(eventId)).toBe(outboxIdempotencyKey(eventId));
  });
});

describe("outboxMessageId", () => {
  const eventId = "6f4b2c8e-5f6a-4c0a-9c3a-2f0d9c8b7a65";

  it("derives the domain from a display-name MAIL_FROM", () => {
    expect(outboxMessageId(eventId, "公共空间细节地图 <noreply@example.test>"))
      .toBe(`<outbox.${eventId}@example.test>`);
  });

  it("derives the domain from a bare address MAIL_FROM", () => {
    expect(outboxMessageId(eventId, "noreply@mail.example.org"))
      .toBe(`<outbox.${eventId}@mail.example.org>`);
  });

  it("falls back to localhost when no domain is present", () => {
    expect(outboxMessageId(eventId, "No Reply")).toBe(`<outbox.${eventId}@localhost>`);
  });

  it("is deterministic per event and unique across events", () => {
    const from = "noreply@example.test";
    expect(outboxMessageId(eventId, from)).toBe(outboxMessageId(eventId, from));
    expect(outboxMessageId(eventId, from)).not.toBe(outboxMessageId("other-event-id", from));
  });
});
