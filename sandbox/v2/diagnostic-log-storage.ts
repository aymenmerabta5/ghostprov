import { SANDBOX_LOG_SEGMENT_BYTES } from "../../src/v2/diagnostics/sandbox-log-schema.ts";
import { isCanonicalSandboxWorkspaceRoot } from "./workspace-root.ts";

export interface DiagnosticFile {
  write(bytes: Uint8Array): number;
  truncate(): void;
  rewind(): void;
  close(): void;
}

export interface DiagnosticStorage {
  inspect(path: string): { kind: "file" | "directory" | "other"; size: number } | null;
  mkdir(path: string): void;
  create(path: string): DiagnosticFile;
  remove(path: string): void;
  rename(source: string, target: string): void;
}

export interface DiagnosticSink {
  append(bytes: Uint8Array): void;
  rotate(): void;
  close(): void;
}

/** Fixed six-file layout: supervisor, current child, and previous child pairs. */
function directory(storage: DiagnosticStorage, workspace: string): string {
  if (!isCanonicalSandboxWorkspaceRoot(workspace)) throw new Error("Invalid diagnostic workspace");
  const parent = `${workspace}/.ghostprov-v2`;
  // The launcher supplies a canonical workspace. Child permissions deliberately
  // cover only .ghostprov-v2, so inspecting the workspace's other files is unnecessary.
  if (storage.inspect(parent)?.kind !== "directory") throw new Error("Invalid diagnostic parent");
  const root = `${parent}/diagnostics`;
  if (!storage.inspect(root)) storage.mkdir(root);
  if (storage.inspect(root)?.kind !== "directory") throw new Error("Invalid diagnostic directory");
  return root;
}

export function openDiagnosticSink(storage: DiagnosticStorage, workspace: string, role: "sidecar" | "supervisor"): DiagnosticSink {
  const root = directory(storage, workspace);
  const handles: DiagnosticFile[] = [];
  try {
    for (const suffix of ["", ".1"]) handles.push(storage.create(`${root}/${role}${suffix}.jsonl`));
  } catch (error) {
    for (const handle of handles) handle.close();
    throw error;
  }
  let active = 0;
  let closed = false;
  return {
    append(bytes) {
      if (closed || bytes.length > SANDBOX_LOG_SEGMENT_BYTES) throw new Error("Diagnostic sink unavailable");
      for (let offset = 0; offset < bytes.length;) {
        const written = handles[active]!.write(bytes.subarray(offset));
        if (!Number.isSafeInteger(written) || written < 1 || written > bytes.length - offset) throw new Error("Diagnostic write made no progress");
        offset += written;
      }
    },
    rotate() {
      if (closed) throw new Error("Diagnostic sink is closed");
      active = 1 - active;
      // Use the exact open handle even if a directory entry was replaced.
      handles[active]!.truncate();
      handles[active]!.rewind();
    },
    close() {
      if (closed) return;
      closed = true;
      for (const handle of handles) { try { handle.close(); } catch { /* Diagnostic cleanup is best effort. */ } }
    },
  };
}

/** Call only after exact retained-child exit proof, immediately before a new launch. */
export function archiveChildDiagnosticFiles(storage: DiagnosticStorage, workspace: string): void {
  const root = directory(storage, workspace);
  const pairs = ["", ".1"].map(suffix => ({ current: `${root}/sidecar${suffix}.jsonl`, previous: `${root}/sidecar.previous${suffix}.jsonl` }));
  const states = pairs.map(pair => ({ ...pair, currentState: storage.inspect(pair.current), previousState: storage.inspect(pair.previous) }));
  for (const pair of states) {
    for (const state of [pair.currentState, pair.previousState]) {
      if (state && (state.kind !== "file" || state.size > SANDBOX_LOG_SEGMENT_BYTES)) throw new Error("Diagnostic archive identity is invalid");
    }
  }
  // Preserve the last failed child's logs when a later launch never opened a log.
  if (!states.some(pair => pair.currentState)) return;
  for (const pair of states) {
    if (pair.previousState) storage.remove(pair.previous);
    if (pair.currentState) storage.rename(pair.current, pair.previous);
  }
}

export function denoDiagnosticStorage(): DiagnosticStorage {
  return {
    inspect(path) {
      try {
        const stat = Deno.lstatSync(path);
        return { kind: stat.isSymlink ? "other" : stat.isFile ? "file" : stat.isDirectory ? "directory" : "other", size: stat.size };
      } catch (error) {
        if (error instanceof Deno.errors.NotFound) return null;
        throw error;
      }
    },
    mkdir(path) { Deno.mkdirSync(path, { mode: 0o700 }); },
    create(path) {
      const file = Deno.openSync(path, { write: true, createNew: true, mode: 0o600 });
      return { write: bytes => file.writeSync(bytes), truncate: () => file.truncateSync(0),
        rewind: () => { file.seekSync(0, Deno.SeekMode.Start); }, close: () => file.close() };
    },
    remove(path) { Deno.removeSync(path); },
    rename(source, target) { Deno.renameSync(source, target); },
  };
}
