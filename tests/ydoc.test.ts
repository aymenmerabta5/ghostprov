import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { tombstoneYDocEntry } from "../src/sandbox/ydoc";

describe("Yjs tracked-file cleanup", () => {
  it("tombstones an entry and clears its content atomically", () => {
    const doc = new Y.Doc();
    const entry = new Y.Map<any>();
    doc.getMap("entries").set("entry-1", entry);
    const content = new Y.Text();
    content.insert(0, "segment payload");
    entry.set("deleted", false);
    entry.set("content", content);

    tombstoneYDocEntry(doc, entry);

    expect(entry.get("deleted")).toBe(true);
    expect((entry.get("content") as Y.Text).toString()).toBe("");
  });
});
