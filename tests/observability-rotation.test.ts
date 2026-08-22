import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { RotatingJsonlSink } from "../src/observability/rotating-jsonl";

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => rm(directory, { recursive: true, force: true })));
});

describe("RotatingJsonlSink", () => {
  it("rotates before the byte ceiling and retains only exact matching files", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-logs-"));
    directories.push(directory);
    await writeFile(path.join(directory, "unrelated.jsonl"), "keep me");
    const sink = new RotatingJsonlSink({
      directory,
      prefix: "ghostprov-v2",
      rotateBytes: 180,
      retainFiles: 2,
      pid: 123,
      now: (() => {
        let value = Date.UTC(2026, 7, 22);
        return () => value += 1_000;
      })(),
    });

    for (let index = 0; index < 8; index++) {
      await sink.writeBatch([`${JSON.stringify({ index, value: "x".repeat(70) })}\n`]);
    }
    await sink.close();

    const names = await readdir(directory);
    const rotated = names.filter(name => /^ghostprov-v2\.\d{8}T\d{9}Z\.123\.\d+\.jsonl$/.test(name));
    expect(rotated).toHaveLength(2);
    expect(names).toContain("unrelated.jsonl");
    expect(names).toContain("ghostprov-v2.current.123.jsonl");
    for (const name of [...rotated, "ghostprov-v2.current.123.jsonl"]) {
      const text = await readFile(path.join(directory, name), "utf8");
      for (const line of text.trim().split("\n").filter(Boolean)) expect(() => JSON.parse(line)).not.toThrow();
    }
  });
});
