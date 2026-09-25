import { describe, expect, it } from "vitest";
import { classifySmtpError } from "./classify";
import { backoffSeconds, deliverClaimedEvent, deliveryMessageId, type MachineDeps } from "./machine";
import { messageIdDomain, type Mailer, type MailInput } from "./mailer";
import {
  CLAIM_NEXT_SQL,
  MARK_FAILURE_SQL,
  MARK_SENT_SQL,
  PARK_SQL,
  RECONCILE_SENDING_SQL,
  RECORD_SENDING_SQL,
  type ClaimedEvent,
  type FailureKind,
  type OutboxStore
} from "./store";

type FakeStatus = "pending" | "processing" | "processed" | "failed";

type FakeEventRow = {
  id: string;
  deliveryKey: string;
  claimToken: string | null;
  claimedBy: string | null;
  status: FakeStatus;
  payload: unknown;
  attempts: number;
  ambiguousCount: number;
  availableAt: number;
  leaseExpiresAt: number | null;
  lastError: string | null;
};

type FakeAttemptRow = {
  attemptId: string;
  eventId: string;
  attemptNo: number;
  messageId: string;
  status: "sending" | "sent" | "failed" | "ambiguous";
  error: string | null;
};

class FakeStore implements OutboxStore {
  readonly events = new Map<string, FakeEventRow>();
  readonly attempts: FakeAttemptRow[] = [];
  now = 1_700_000_000_000;
  leaseSeconds = 120;
  crashOnMarkSent = false;
  private tokenSeq = 0;
  private attemptSeq = 0;

  addEvent(input: { id: string; payload: unknown; status?: FakeStatus; availableAt?: number }): FakeEventRow {
    const row: FakeEventRow = {
      id: input.id,
      deliveryKey: `dk-${input.id}`,
      claimToken: null,
      claimedBy: null,
      status: input.status ?? "pending",
      payload: input.payload,
      attempts: 0,
      ambiguousCount: 0,
      availableAt: input.availableAt ?? this.now,
      leaseExpiresAt: null,
      lastError: null
    };
    this.events.set(row.id, row);
    return row;
  }

  private fenced(event: ClaimedEvent): FakeEventRow | null {
    const row = this.events.get(event.id);
    if (!row || row.claimToken !== event.claimToken) return null;
    return row;
  }

  async claimNext(input: { ownerId: string; eventId?: string | undefined }): Promise<ClaimedEvent | null> {
    const row = [...this.events.values()].find((candidate) => {
      if (input.eventId && candidate.id !== input.eventId) return false;
      if (candidate.status === "pending") return candidate.availableAt <= this.now;
      if (candidate.status === "processing") {
        return candidate.leaseExpiresAt !== null && candidate.leaseExpiresAt < this.now;
      }
      return false;
    });
    if (!row) return null;
    row.status = "processing";
    row.claimedBy = input.ownerId;
    row.claimToken = `claim-${++this.tokenSeq}`;
    row.leaseExpiresAt = this.now + this.leaseSeconds * 1000;
    return {
      id: row.id,
      deliveryKey: row.deliveryKey,
      claimToken: row.claimToken,
      payload: row.payload,
      attempts: row.attempts,
      ambiguousCount: row.ambiguousCount
    };
  }

  async reconcileSending(event: ClaimedEvent): Promise<number | null> {
    const row = this.fenced(event);
    if (!row) return null;
    for (const attempt of this.attempts) {
      if (attempt.eventId === row.id && attempt.status === "sending") {
        attempt.status = "ambiguous";
        attempt.error = "Previous delivery attempt ended without a recorded outcome";
        row.ambiguousCount += 1;
      }
    }
    return row.ambiguousCount;
  }

  async recordSending(
    event: ClaimedEvent,
    input: { messageId: string }
  ): Promise<{ attemptId: string; attemptNo: number } | null> {
    const row = this.fenced(event);
    if (!row) return null;
    const attemptNo =
      Math.max(0, ...this.attempts.filter((attempt) => attempt.eventId === row.id).map((attempt) => attempt.attemptNo)) +
      1;
    const attemptId = `attempt-${++this.attemptSeq}`;
    this.attempts.push({
      attemptId,
      eventId: row.id,
      attemptNo,
      messageId: input.messageId,
      status: "sending",
      error: null
    });
    return { attemptId, attemptNo };
  }

  async markSent(event: ClaimedEvent, input: { attemptId: string }): Promise<boolean> {
    if (this.crashOnMarkSent) {
      this.crashOnMarkSent = false;
      throw new Error("simulated crash before delivery confirmation");
    }
    const row = this.fenced(event);
    if (!row) return false;
    const attempt = this.attempts.find((candidate) => candidate.attemptId === input.attemptId);
    if (attempt) attempt.status = "sent";
    row.status = "processed";
    row.leaseExpiresAt = null;
    return true;
  }

  async markFailure(
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
  ): Promise<boolean> {
    const row = this.fenced(event);
    if (!row) return false;
    const attempt = this.attempts.find((candidate) => candidate.attemptId === input.attemptId);
    if (attempt) {
      attempt.status = input.kind;
      attempt.error = input.error;
    }
    row.status = input.terminal ? "failed" : "pending";
    row.attempts = input.attempts;
    row.ambiguousCount = input.ambiguousCount;
    row.lastError = input.error;
    row.leaseExpiresAt = null;
    if (!input.terminal) row.availableAt = this.now + input.retryAfterSeconds * 1000;
    return true;
  }

  async park(event: ClaimedEvent, error: string): Promise<boolean> {
    const row = this.fenced(event);
    if (!row) return false;
    row.status = "failed";
    row.lastError = error;
    row.leaseExpiresAt = null;
    return true;
  }
}

class FakeMailer implements Mailer {
  readonly sends: MailInput[] = [];
  private readonly acceptedMessageIds = new Set<string>();
  // Scripted per-send behavior. The default accepts the mail; a script may
  // accept first (server queued it) and then throw (connection dropped).
  onSend: ((mailer: FakeMailer, input: MailInput) => void) | null = null;

  accept(input: MailInput): void {
    this.acceptedMessageIds.add(input.messageId);
  }

  async send(input: MailInput): Promise<void> {
    this.sends.push(input);
    if (this.onSend) this.onSend(this, input);
    else this.accept(input);
  }

  get effectiveDeliveries(): number {
    return this.acceptedMessageIds.size;
  }
}

function makeDeps(store: FakeStore, mailer: FakeMailer, overrides: Partial<MachineDeps> = {}): MachineDeps {
  return {
    store,
    mailer,
    maxAttempts: 5,
    maxAmbiguous: 2,
    messageIdDomain: "map.test",
    ...overrides
  };
}

const payload = { to: "user@example.test", subject: "你好", text: "正文", html: "<p>正文</p>" };

async function claimAndDeliver(
  deps: MachineDeps,
  store: FakeStore,
  ownerId = "owner-a"
): Promise<{ result: Awaited<ReturnType<typeof deliverClaimedEvent>>; event: ClaimedEvent }> {
  const event = await store.claimNext({ ownerId });
  if (!event) throw new Error("no claimable event");
  return { result: await deliverClaimedEvent(deps, event), event };
}

describe("idempotent outbox delivery", () => {
  it("delivers a pending event once and keeps the original payload intact", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    const deps = makeDeps(store, mailer);

    const { result } = await claimAndDeliver(deps, store);

    expect(result).toBe("sent");
    expect(mailer.sends).toHaveLength(1);
    expect(mailer.sends[0]!.messageId).toBe(deliveryMessageId("dk-evt-1", "map.test"));
    expect(mailer.sends[0]!.headers["X-Outbox-Delivery-Key"]).toBe("dk-evt-1");
    const row = store.events.get("evt-1")!;
    expect(row.status).toBe("processed");
    expect(row.payload).toEqual(payload);
    expect(store.attempts.map((attempt) => attempt.status)).toEqual(["sent"]);
  });

  it("replays a crash before confirmation after lease expiry with the same delivery identity", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    const deps = makeDeps(store, mailer);

    // Instance A: SMTP accepts the mail, then the worker crashes before it can
    // record the confirmation.
    const first = await store.claimNext({ ownerId: "owner-a" });
    store.crashOnMarkSent = true;
    await expect(deliverClaimedEvent(deps, first!)).rejects.toThrow("simulated crash");
    expect(mailer.sends).toHaveLength(1);
    expect(store.events.get("evt-1")!.status).toBe("processing");

    // The event is not replayable while A's lease is still live.
    expect(await store.claimNext({ ownerId: "owner-b" })).toBeNull();

    // After the lease expires, instance B reconciles the interrupted attempt
    // and replays with the same Message-ID.
    store.now += 121_000;
    const { result } = await claimAndDeliver(deps, store, "owner-b");

    expect(result).toBe("sent");
    expect(mailer.sends).toHaveLength(2);
    expect(mailer.sends[1]!.messageId).toBe(mailer.sends[0]!.messageId);
    // A dedup-capable SMTP provider collapses the replay to one delivery.
    expect(mailer.effectiveDeliveries).toBe(1);
    const row = store.events.get("evt-1")!;
    expect(row.status).toBe("processed");
    expect(row.ambiguousCount).toBe(1);
    expect(store.attempts.map((attempt) => attempt.status)).toEqual(["ambiguous", "sent"]);
    expect(row.payload).toEqual(payload);
  });

  it("only claims scheduled pending rows and expired leases", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "future", payload, availableAt: store.now + 60_000 });
    expect(await store.claimNext({ ownerId: "owner-a" })).toBeNull();

    store.now += 61_000;
    const claimed = await store.claimNext({ ownerId: "owner-a" });
    expect(claimed?.id).toBe("future");

    // A live lease held by another instance is not claimable.
    expect(await store.claimNext({ ownerId: "owner-b" })).toBeNull();
    store.now += 121_000;
    const reclaimed = await store.claimNext({ ownerId: "owner-b" });
    expect(reclaimed?.id).toBe("future");
    expect(reclaimed?.claimToken).not.toBe(claimed?.claimToken);
  });

  it("rejects the stale owner's writes with the fencing token after lease expiry", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    const deps = makeDeps(store, mailer);

    const stale = await store.claimNext({ ownerId: "owner-a" });
    store.now += 121_000;
    const fresh = await store.claimNext({ ownerId: "owner-b" });
    expect(fresh!.claimToken).not.toBe(stale!.claimToken);

    // The stale owner lost the lease: reconciliation fails before any new send.
    expect(await deliverClaimedEvent(deps, stale!)).toBe("fence-lost");
    expect(mailer.sends).toHaveLength(0);

    // The new owner delivers normally.
    expect(await deliverClaimedEvent(deps, fresh!)).toBe("sent");
    expect(mailer.sends).toHaveLength(1);
    expect(store.events.get("evt-1")!.status).toBe("processed");
  });

  it("retries definite failures on the normal budget without spending ambiguous retries", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    mailer.onSend = () => {
      throw Object.assign(new Error("535 Invalid credentials"), { code: "EAUTH" });
    };
    const deps = makeDeps(store, mailer, { maxAttempts: 3 });

    expect((await claimAndDeliver(deps, store)).result).toBe("retry");
    expect(store.events.get("evt-1")!.status).toBe("pending");
    store.now += 3_000;
    expect((await claimAndDeliver(deps, store)).result).toBe("retry");
    store.now += 5_000;
    expect((await claimAndDeliver(deps, store)).result).toBe("parked");

    expect(mailer.sends).toHaveLength(3);
    const row = store.events.get("evt-1")!;
    expect(row.status).toBe("failed");
    expect(row.attempts).toBe(3);
    expect(row.ambiguousCount).toBe(0);
    expect(store.attempts.every((attempt) => attempt.status === "failed")).toBe(true);
  });

  it("parks ambiguous outcomes once the replay budget is exhausted", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    // The server may have queued the mail before the socket died.
    mailer.onSend = (sender, input) => {
      sender.accept(input);
      throw Object.assign(new Error("socket closed"), { code: "ESOCKET" });
    };
    const deps = makeDeps(store, mailer, { maxAmbiguous: 2 });

    expect((await claimAndDeliver(deps, store)).result).toBe("retry");
    store.now += 3_000;
    expect((await claimAndDeliver(deps, store)).result).toBe("retry");
    store.now += 5_000;
    expect((await claimAndDeliver(deps, store)).result).toBe("parked");

    // Every physical attempt carries the same Message-ID; the duplicate window
    // is bounded and fully recorded in the attempt ledger.
    expect(mailer.sends).toHaveLength(3);
    expect(new Set(mailer.sends.map((input) => input.messageId)).size).toBe(1);
    const row = store.events.get("evt-1")!;
    expect(row.status).toBe("failed");
    expect(row.ambiguousCount).toBe(3);
    expect(store.attempts.every((attempt) => attempt.status === "ambiguous")).toBe(true);
  });

  it("parks without resending when crashed attempts already exhausted the ambiguous budget", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-1", payload });
    const mailer = new FakeMailer();
    const deps = makeDeps(store, mailer, { maxAmbiguous: 0 });

    const first = await store.claimNext({ ownerId: "owner-a" });
    store.crashOnMarkSent = true;
    await expect(deliverClaimedEvent(deps, first!)).rejects.toThrow("simulated crash");
    expect(mailer.sends).toHaveLength(1);

    store.now += 121_000;
    const { result } = await claimAndDeliver(deps, store, "owner-b");
    expect(result).toBe("parked");
    expect(mailer.sends).toHaveLength(1);
    expect(store.events.get("evt-1")!.status).toBe("failed");
  });

  it("parks events whose payload is not a deliverable email", async () => {
    const store = new FakeStore();
    store.addEvent({ id: "evt-bad", payload: { unexpected: true } });
    const mailer = new FakeMailer();
    const deps = makeDeps(store, mailer);

    const { result } = await claimAndDeliver(deps, store);
    expect(result).toBe("parked");
    expect(mailer.sends).toHaveLength(0);
    expect(store.events.get("evt-bad")!.status).toBe("failed");
  });
});

describe("classifySmtpError", () => {
  it("treats explicit SMTP replies and unopened connections as definite failures", () => {
    expect(classifySmtpError(Object.assign(new Error("550 rejected"), { responseCode: 550 }))).toBe("failed");
    expect(classifySmtpError(Object.assign(new Error("auth"), { code: "EAUTH" }))).toBe("failed");
    expect(classifySmtpError(Object.assign(new Error("env"), { code: "EENVELOPE" }))).toBe("failed");
    expect(classifySmtpError(Object.assign(new Error("connect"), { code: "ECONNECTION" }))).toBe("failed");
  });

  it("treats mid-session transport errors and unknown errors as ambiguous", () => {
    expect(classifySmtpError(Object.assign(new Error("socket"), { code: "ESOCKET" }))).toBe("ambiguous");
    expect(classifySmtpError(Object.assign(new Error("timed out"), { code: "ETIMEDOUT" }))).toBe("ambiguous");
    expect(classifySmtpError(new Error("unexpected"))).toBe("ambiguous");
  });

  it("derives a stable backoff and Message-ID", () => {
    expect(backoffSeconds(1)).toBe(2);
    expect(backoffSeconds(4)).toBe(16);
    expect(backoffSeconds(9)).toBe(300);
    expect(deliveryMessageId("abc", "map.test")).toBe("<abc@map.test>");
    expect(messageIdDomain("地图 <noreply@example.test>")).toBe("example.test");
    expect(messageIdDomain("noreply@example.test")).toBe("example.test");
    expect(messageIdDomain("garbage")).toBe("map.local");
  });
});

describe("outbox store SQL", () => {
  it("claims across instances only through the lease boundary", () => {
    expect(CLAIM_NEXT_SQL).toContain("FOR UPDATE SKIP LOCKED");
    expect(CLAIM_NEXT_SQL).toContain("status = 'pending'");
    expect(CLAIM_NEXT_SQL).toContain("status = 'processing'");
    expect(CLAIM_NEXT_SQL).toContain("lease_expires_at");
    expect(CLAIM_NEXT_SQL).toContain("claim_token");
  });

  it("fences every state transition by the claim token", () => {
    for (const sql of [
      RECONCILE_SENDING_SQL,
      RECORD_SENDING_SQL,
      MARK_SENT_SQL,
      MARK_FAILURE_SQL,
      PARK_SQL
    ]) {
      expect(sql).toContain("claim_token");
    }
  });

  it("never overwrites the original payload on success so history stays recomputable", () => {
    expect(MARK_SENT_SQL).not.toContain("payload");
  });
});
