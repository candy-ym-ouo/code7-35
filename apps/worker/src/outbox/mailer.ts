import nodemailer from "nodemailer";
import type { Transporter } from "nodemailer";

export type MailInput = {
  to: string;
  subject: string;
  text: string;
  html: string;
  // Stable, deterministic identity derived from the outbox delivery key. It is
  // reused on every replay so dedup-capable SMTP providers collapse replays.
  messageId: string;
  headers: Record<string, string>;
};

export interface Mailer {
  send(input: MailInput): Promise<void>;
}

export function messageIdDomain(mailFrom: string): string {
  const match = mailFrom.match(/@([A-Za-z0-9][A-Za-z0-9.-]*)/);
  return match?.[1] ?? "map.local";
}

export function createSmtpMailer(options: {
  host: string;
  port: number;
  secure: boolean;
  user?: string | undefined;
  password: string | undefined;
  from: string;
  // Applies to connection, greeting and socket phases so a single send can
  // never outlive the outbox lease.
  timeoutMs: number;
}): Mailer {
  const transporter: Transporter = nodemailer.createTransport({
    host: options.host,
    port: options.port,
    secure: options.secure,
    auth: options.user ? { user: options.user, pass: options.password } : undefined,
    connectionTimeout: options.timeoutMs,
    greetingTimeout: options.timeoutMs,
    socketTimeout: options.timeoutMs
  });

  return {
    async send(input) {
      await transporter.sendMail({
        from: options.from,
        to: input.to,
        subject: input.subject,
        text: input.text,
        html: input.html,
        messageId: input.messageId,
        headers: input.headers
      });
    }
  };
}
