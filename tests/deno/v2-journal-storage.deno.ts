import { DenoJournalStorage } from "../../sandbox/v2/journal-storage.ts";
import { DenoSidecarOwnershipLease } from "../../sandbox/v2/ownership-lease.ts";
import { phaseZeroBootstrap } from "../../sandbox/v2/bootstrap.ts";
import { IncrementalSha256 } from "../../src/v2/protocol/transcript.ts";
import {
  journalStoredRecordBytes,
  type JournalRecord,
} from "../../src/v2/sidecar/journal.ts";
import {
  V2_TERMINAL_SPOOL_CAPACITY,
  V2_TERMINAL_SPOOL_RESERVATION_BYTES,
} from "../../src/v2/sidecar/capacity.ts";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function assertRejects(
  operation: () => Promise<unknown>,
  pattern: RegExp,
): Promise<void> {
  try {
    await operation();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    assert(pattern.test(message), `unexpected rejection: ${message}`);
    return;
  }
  throw new Error("expected operation to reject");
}

async function withStorage(
  operation: (input: {
    root: string;
    journalRoot: string;
    storage: DenoJournalStorage;
  }) => Promise<void>,
  options: {
    maxTerminalSpoolBytes?: number;
    maxTerminalBytes?: number;
    maxActiveSpools?: number;
    maxJournalBytes?: number;
    maxJournalRecords?: number;
    syncDirectory?: (path: string) => Promise<void>;
  } = {},
): Promise<void> {
  const root = await Deno.makeTempDir({ prefix: "ghostprov-deno-journal-" });
  // The production sidecar is Linux. Windows Deno does not expose stable
  // dev/inode identity, so its native-FS pass exercises storage with the same
  // serialized ownership callback while the Linux pass exercises the real
  // OS lock and identity checks as well.
  const lease = Deno.build.os === "windows"
    ? null
    : await DenoSidecarOwnershipLease.acquire({
        instanceId: crypto.randomUUID(),
        root,
      });
  const owned = <T>(task: () => Promise<T>): Promise<T> => (
    lease ? lease.withOwnership(task) : task()
  );
  const journalRoot = `${root}/journals/r2`;
  const storage = new DenoJournalStorage({
    root: journalRoot,
    maxTerminalSpoolBytes: options.maxTerminalSpoolBytes,
    maxTerminalBytes: options.maxTerminalBytes,
    maxActiveSpools: options.maxActiveSpools,
    maxJournalBytes: options.maxJournalBytes,
    maxJournalRecords: options.maxJournalRecords,
    syncDirectory: options.syncDirectory,
    assertExclusiveOwnership: () => lease ? lease.assertOwned() : Promise.resolve(),
    withExclusiveOwnership: owned,
  });
  try {
    await operation({ root, journalRoot, storage });
  } finally {
    await lease?.release().catch(() => undefined);
    await Deno.remove(root, { recursive: true }).catch(() => undefined);
  }
}

Deno.test({
  name: "owned sidecar atomically promotes only its exact candidate process identity",
  ignore: Deno.build.os === "windows",
  fn: async () => {
    const root = await Deno.makeTempDir({ prefix: "ghostprov-deno-owner-pid-" });
    const launchNonce = "deno-owner-launch";
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: crypto.randomUUID(),
      root,
    });
    try {
      const start = "987654321";
      const record = `${Deno.pid} ${launchNonce} ${start}\n`;
      const candidate = `${root}/sidecar.pid.${launchNonce}.${Deno.pid}.candidate`;
      const staleRecord = `${Deno.pid} ${launchNonce} ${BigInt(start) + 1n}\n`;
      await Deno.writeTextFile(candidate, staleRecord, { createNew: true, mode: 0o600 });
      await assertRejects(
        () => lease.promoteProcessIdentity(launchNonce, start),
        /promote.*process identity/i,
      );
      await assertRejects(
        () => Deno.stat(`${root}/sidecar.pid`),
        /not found|cannot find|no such file/i,
      );
      await lease.assertOwned();

      await Deno.writeTextFile(candidate, record);

      await lease.promoteProcessIdentity(launchNonce, start);

      assert(await Deno.readTextFile(`${root}/sidecar.pid`) === record, "canonical PID changed");
      await assertRejects(() => Deno.stat(candidate), /not found|cannot find|no such file/i);
      await lease.assertOwned();
    } finally {
      await lease.release().catch(() => undefined);
      await Deno.remove(root, { recursive: true }).catch(() => undefined);
    }
  },
});

Deno.test("PID promotion failure never publishes a bootstrap hello", async () => {
  const root = await Deno.makeTempDir({ prefix: "ghostprov-deno-bootstrap-promotion-" });
  const helloPath = `${root}/bootstrap-hello`;
  try {
    await assertRejects(() => phaseZeroBootstrap({
      binding: {
        projectId: "e631d12c-d917-4434-b360-41b4715d6679",
        sandboxSessionId: "sandbox-session-promotion-fault",
        leaderGeneration: 1,
        buildHash: "a".repeat(64),
        schemaHash: "b".repeat(64),
        nonce: "launch-promotion-fault",
        protocolRevision: "r2",
        schemaVersion: 2,
      },
      instanceId: "sidecar-promotion-fault",
      encryptedPath: `${root}/bootstrap.enc.json`,
      helloPath,
      beforeHelloPublished: async () => {
        throw new Error("synthetic PID promotion failure");
      },
    }), /synthetic PID promotion failure/i);
    await assertRejects(
      () => Deno.stat(helloPath),
      /not found|cannot find|no such file/i,
    );
  } finally {
    await Deno.remove(root, { recursive: true }).catch(() => undefined);
  }
});

Deno.test("real Deno journal storage atomically spools, replaces, bounds reads, and prunes", async () => {
  await withStorage(async ({ journalRoot, storage }) => {
    const requestId = "req-deno-native";
    const firstBytes = new TextEncoder().encode("event: response.output_text.delta\ndata: first\n\n");
    const firstHash = new IncrementalSha256().update(firstBytes).digestHex();
    const first = await storage.beginTerminalSpool(requestId, 256);
    await first.append(firstBytes.subarray(0, 7));
    await first.append(firstBytes.subarray(7));
    await first.promote({ byteCount: firstBytes.byteLength, transcriptSha256: firstHash });

    const terminalPath = `${journalRoot}/terminals/${requestId}.json`;
    const firstStat = await Deno.stat(terminalPath);
    assert(firstStat.isFile && firstStat.size === firstBytes.byteLength, "first spool size is wrong");
    if (Deno.build.os !== "windows" && firstStat.mode !== null) {
      assert((firstStat.mode & 0o777) === 0o600, "terminal spool permissions are not 0600");
    }
    const recovered = await storage.readTerminal(requestId, 256);
    assert(recovered !== null, "promoted terminal was not readable");
    assert(new TextDecoder().decode(recovered) === new TextDecoder().decode(firstBytes), "terminal bytes changed");
    await assertRejects(() => storage.readTerminal(requestId, firstBytes.byteLength - 1), /recovery byte limit/i);

    const replacementBytes = new TextEncoder().encode("event: response.completed\ndata: replacement\n\n");
    const replacementHash = new IncrementalSha256().update(replacementBytes).digestHex();
    const replacement = await storage.beginTerminalSpool(requestId, 256);
    await replacement.append(replacementBytes);
    await replacement.promote({
      byteCount: replacementBytes.byteLength,
      transcriptSha256: replacementHash,
    });
    assert((await Deno.stat(terminalPath)).size === replacementBytes.byteLength, "atomic replacement size is wrong");

    await Deno.writeTextFile(`${journalRoot}/terminals/orphan.crash.tmp`, "orphan", {
      createNew: true,
      mode: 0o600,
    });
    assert(typeof storage.pruneTerminals === "function", "owned storage has no prune operation");
    await storage.pruneTerminals!([]);
    await assertRejects(() => Deno.stat(terminalPath), /not found|cannot find/i);
    await assertRejects(
      () => Deno.stat(`${journalRoot}/terminals/orphan.crash.tmp`),
      /not found|cannot find/i,
    );
  }, {
    maxTerminalSpoolBytes: 256,
    maxTerminalBytes: 512,
    maxActiveSpools: 4,
  });
});

Deno.test("default spool quota reserves exactly eight worst-case prepared requests", async () => {
  await withStorage(async ({ storage }) => {
    const spools: Awaited<ReturnType<DenoJournalStorage["beginTerminalSpool"]>>[] = [];
    try {
      for (let index = 0; index < V2_TERMINAL_SPOOL_CAPACITY; index++) {
        spools.push(await storage.beginTerminalSpool(
          `req-capacity-${index}`,
          V2_TERMINAL_SPOOL_RESERVATION_BYTES,
        ));
      }
      assert(spools.length === 8, "default advertised spool capacity is not eight");
      await assertRejects(
        () => storage.beginTerminalSpool(
          "req-capacity-9",
          V2_TERMINAL_SPOOL_RESERVATION_BYTES,
        ),
        /capacity|quota|concurrency/i,
      );
    } finally {
      await Promise.all(spools.map(spool => spool.abort()));
    }
  });
});

Deno.test("first journal append survives restart and runtime limits reject before write", async () => {
  const first: JournalRecord = {
    version: 1,
    sequence: 1,
    requestId: "req-first-directory-fence",
    requestDigest: "a".repeat(64),
    leaderGeneration: 1,
    state: "claimed",
    recordedAtMs: 1,
  };
  const exactBytes = journalStoredRecordBytes(first);
  await withStorage(async ({ journalRoot, storage }) => {
    await storage.appendAndSync([first]);
    const path = `${journalRoot}/claims.jsonl`;
    assert((await Deno.stat(path)).size === exactBytes, "first journal append byte count diverged");

    const second: JournalRecord = {
      ...first,
      sequence: 2,
      requestId: "req-must-not-append",
      requestDigest: "b".repeat(64),
    };
    await assertRejects(() => storage.appendAndSync([second]), /runtime capacity/i);
    assert((await Deno.stat(path)).size === exactBytes, "capacity failure modified the journal");

    const restarted = new DenoJournalStorage({
      root: journalRoot,
      maxJournalBytes: exactBytes,
      maxJournalRecords: 1,
      withExclusiveOwnership: async operation => operation(),
    });
    const recovered = await restarted.readRecords();
    assert(recovered.length === 1, "restart did not recover the first append");
    assert(recovered[0]?.requestId === first.requestId, "restart recovered the wrong claim");
  }, {
    maxJournalBytes: exactBytes,
    maxJournalRecords: 1,
  });
});

Deno.test("first journal append does not resolve when the parent-directory fence fails", async () => {
  const first: JournalRecord = {
    version: 1,
    sequence: 1,
    requestId: "req-directory-fault",
    requestDigest: "c".repeat(64),
    leaderGeneration: 1,
    state: "claimed",
    recordedAtMs: 1,
  };
  let journalRootSyncs = 0;
  const failureSync = Deno.build.os === "windows" ? 1 : 2;
  await withStorage(async ({ journalRoot, storage }) => {
    await assertRejects(() => storage.appendAndSync([first]), /synthetic directory fsync failure/i);
    const restarted = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    const recovered = await restarted.readRecords();
    // The file may or may not survive a real crash after the failed fence. If it
    // does, its claim remains authoritative and prevents a duplicate fetch.
    assert(recovered.length === 1, "failed directory fence lost the readable claim in the fault model");
    assert(recovered[0]?.requestId === first.requestId, "fault-model recovery changed the claim");
  }, {
    syncDirectory: async path => {
      if (!path.endsWith("/journals/r2")) return;
      journalRootSyncs++;
      if (journalRootSyncs === failureSync) throw new Error("synthetic directory fsync failure");
    },
  });
});

Deno.test("rename durability fences fail-stop journal replacement and terminal promotion", async () => {
  const first: JournalRecord = {
    version: 1,
    sequence: 1,
    requestId: "req-rename-fence",
    requestDigest: "d".repeat(64),
    leaderGeneration: 1,
    state: "claimed",
    recordedAtMs: 1,
  };
  let failRoot = false;
  let failTerminals = false;
  await withStorage(async ({ journalRoot, storage }) => {
    await storage.appendAndSync([first]);
    assert(typeof storage.replaceRecords === "function", "owned storage has no replacement");
    failRoot = true;
    await assertRejects(() => storage.replaceRecords!([{ ...first, compacted: true }]), /replace fsync failure/i);

    const bytes = new TextEncoder().encode("terminal rename fence");
    const spool = await storage.beginTerminalSpool("req-terminal-rename", 256);
    await spool.append(bytes);
    failTerminals = true;
    await assertRejects(() => spool.promote({
      byteCount: bytes.byteLength,
      transcriptSha256: new IncrementalSha256().update(bytes).digestHex(),
    }), /terminal fsync failure/i);

    const restarted = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    assert((await restarted.readRecords()).length === 1, "failed replacement lost the claim fence");
    assert(await restarted.terminalExists("req-terminal-rename"), "failed promotion lost its spool fence");
  }, {
    syncDirectory: async path => {
      if (failRoot && path.endsWith("/journals/r2")) {
        failRoot = false;
        throw new Error("synthetic replace fsync failure");
      }
      if (failTerminals && path.endsWith("/journals/r2/terminals")) {
        failTerminals = false;
        throw new Error("synthetic terminal fsync failure");
      }
    },
  });
});

Deno.test("repairs a torn multibyte UTF-8 tail but rejects invalid UTF-8 in a complete line", async () => {
  const first: JournalRecord = {
    version: 1,
    sequence: 1,
    requestId: "req-utf8-tail",
    requestDigest: "e".repeat(64),
    leaderGeneration: 1,
    state: "claimed",
    recordedAtMs: 1,
  };
  await withStorage(async ({ journalRoot, storage }) => {
    await storage.appendAndSync([first]);
    const path = `${journalRoot}/claims.jsonl`;
    const provenBytes = (await Deno.stat(path)).size;
    // Two bytes of a three-byte UTF-8 code point model a crash in provider text.
    await Deno.writeFile(path, Uint8Array.of(0xe2, 0x82), { append: true });
    const repaired = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    const records = await repaired.readRecords();
    assert(records.length === 1 && records[0]?.requestId === first.requestId, "torn UTF-8 tail lost prefix");
    assert((await Deno.stat(path)).size === provenBytes, "torn UTF-8 tail was not rewritten away");

    await Deno.writeFile(path, Uint8Array.of(0xff, 0x0a), { append: true });
    const corrupt = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    await assertRejects(() => corrupt.readRecords(), /corruption before trailing append/i);
  });
});

Deno.test("cleans bounded crash rewrite temps and fails closed on temp quota or shape", async () => {
  await withStorage(async ({ journalRoot, storage }) => {
    await storage.readRecords();
    const names = Array.from({ length: 3 }, (_, index) => (
      `claims.jsonl.${100 + index}.00000000-0000-4000-8000-${String(index).padStart(12, "0")}.tmp`
    ));
    for (const name of names) {
      await Deno.writeTextFile(`${journalRoot}/${name}`, `crash-${name}`, { createNew: true });
    }
    const cleanup = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    await cleanup.readRecords();
    for (const name of names) {
      await assertRejects(() => Deno.stat(`${journalRoot}/${name}`), /not found|cannot find/i);
    }

    const oversized = "claims.jsonl.999.00000000-0000-4000-8000-999999999999.tmp";
    await Deno.writeFile(`${journalRoot}/${oversized}`, new Uint8Array(33), { createNew: true });
    const quota = new DenoJournalStorage({
      root: journalRoot,
      maxJournalBytes: 32,
      withExclusiveOwnership: async operation => operation(),
    });
    await assertRejects(() => quota.readRecords(), /temporary byte quota/i);
    await Deno.remove(`${journalRoot}/${oversized}`);

    const unexpected = "claims.jsonl.not-a-valid-rewrite.tmp";
    await Deno.writeTextFile(`${journalRoot}/${unexpected}`, "unexpected", { createNew: true });
    const strict = new DenoJournalStorage({
      root: journalRoot,
      withExclusiveOwnership: async operation => operation(),
    });
    await assertRejects(() => strict.readRecords(), /unexpected journal rewrite temporary/i);
  });
});

Deno.test({
  name: "real Deno journal storage enforces the exact 64 MiB spool boundary",
  ignore: Deno.env.get("GP_DENO_LARGE") !== "1",
  fn: async () => {
    const limit = 64 * 1024 * 1024;
    await withStorage(async ({ journalRoot, storage }) => {
      const requestId = "req-deno-64mib";
      const spool = await storage.beginTerminalSpool(requestId, limit);
      const hasher = new IncrementalSha256();
      const chunk = new Uint8Array(64 * 1024);
      for (let index = 0; index < limit / chunk.byteLength; index++) {
        chunk.fill(index & 0xff);
        hasher.update(chunk);
        await spool.append(chunk);
      }
      assert(spool.byteCount === limit, "64 MiB spool byte accounting is wrong");
      await assertRejects(() => spool.append(new Uint8Array([1])), /limit|quota/i);
      await spool.promote({ byteCount: limit, transcriptSha256: hasher.digestHex() });
      const path = `${journalRoot}/terminals/${requestId}.json`;
      assert((await Deno.stat(path)).size === limit, "64 MiB promoted file size is wrong");
      await storage.deleteTerminal(requestId);
      await assertRejects(() => Deno.stat(path), /not found|cannot find/i);
    }, {
      maxTerminalSpoolBytes: limit,
      maxTerminalBytes: limit,
      maxActiveSpools: 1,
    });
  },
});
