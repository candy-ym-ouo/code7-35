// SMTP errors are split by what they prove about the server:
// - "failed": the server provably never accepted the message (an explicit SMTP
//   rejection, or the connection never came up). Safe to replay freely.
// - "ambiguous": the connection dropped or timed out after the session was
//   established, so the server may have accepted the message. Replays must
//   reuse the same delivery identity and are budget-limited.
const DEFINITE_FAILURE_CODES = new Set([
  "EAUTH", // authentication failed before any recipient was negotiated
  "EENVELOPE", // MAIL FROM / RCPT TO rejected with a definitive reply
  "EMESSAGE", // the message itself was rejected or could not be built
  "EDNS", // MX/A lookup failed, no connection was opened
  "ECONNECTION", // TCP/TLS connect failed, no session was established
  "ECONNREFUSED",
  "ENOTFOUND"
]);

const AMBIGUOUS_CODES = new Set([
  "ESOCKET", // socket error after the session was established
  "ETIMEDOUT", // the server stopped answering mid-session
  "ECONNRESET",
  "EPIPE"
]);

export type SmtpFailureKind = "failed" | "ambiguous";

export function classifySmtpError(error: unknown): SmtpFailureKind {
  if (typeof error === "object" && error !== null) {
    // Any explicit SMTP reply is a definitive answer: nodemailer only resolves
    // after a 250 to end-of-DATA, so a response code means "not accepted".
    const responseCode = (error as { responseCode?: unknown }).responseCode;
    if (typeof responseCode === "number" && responseCode >= 400) return "failed";
    const code = (error as { code?: unknown }).code;
    if (typeof code === "string") {
      if (DEFINITE_FAILURE_CODES.has(code)) return "failed";
      if (AMBIGUOUS_CODES.has(code)) return "ambiguous";
    }
  }
  // Unknown errors stay ambiguous: at-least-once is the safe default.
  return "ambiguous";
}
