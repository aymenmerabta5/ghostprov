import { describe, expect, it } from "bun:test";
import { SupervisorMailbox } from "../src/v2/supervisor/mailbox";

describe("Supervisor control mailbox", () => {
  it("coalesces notification floods into one pending message while one handler runs", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const second = Promise.withResolvers<void>();
    const handled: number[] = [];
    const mailbox = new SupervisorMailbox<{ sequence: number }>(async value => {
      handled.push(value.sequence);
      if (value.sequence === 1) { entered.resolve(); await release.promise; }
      else second.resolve();
    });
    mailbox.offer({ sequence: 1 });
    await entered.promise;
    for (let index = 0; index < 10_000; index++) mailbox.offer({ sequence: 2 });
    mailbox.offer({ sequence: 1 });
    expect(mailbox.snapshot()).toEqual({ active: 1, queued: 1 });
    release.resolve();
    await second.promise;
    await mailbox.close();
    expect(handled).toEqual([1, 2]);
    expect(mailbox.snapshot()).toEqual({ active: 0, queued: 0 });
  });

  it("drops pending work on close and joins only the active handler", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const handled: number[] = [];
    const mailbox = new SupervisorMailbox<{ sequence: number }>(async value => {
      handled.push(value.sequence);
      entered.resolve();
      await release.promise;
    });
    mailbox.offer({ sequence: 1 });
    await entered.promise;
    mailbox.offer({ sequence: 2 });
    const closing = mailbox.close();
    mailbox.offer({ sequence: 3 });
    expect(mailbox.snapshot()).toEqual({ active: 1, queued: 0 });
    release.resolve();
    await closing;
    expect(handled).toEqual([1]);
  });

  it("allows the next message after verification throws", async () => {
    const failed = Promise.withResolvers<void>();
    const completed = Promise.withResolvers<void>();
    const mailbox = new SupervisorMailbox<{ sequence: number }>(async value => {
      if (value.sequence === 1) { failed.resolve(); throw new Error("invalid test signature"); }
      completed.resolve();
    });
    mailbox.offer({ sequence: 1 });
    await failed.promise;
    mailbox.offer({ sequence: 2 });
    await completed.promise;
    await mailbox.close();
  });
});
