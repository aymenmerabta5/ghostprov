import { describe, expect, it } from "bun:test";
import { ApiError } from "../src/http/errors";
import {
  collectRequestBody,
  contentLengthExceeds,
  declaredContentLength,
} from "../src/http/body";

describe("bounded HTTP request collection", () => {
  it("combines chunks without copying beyond the final Buffer allocation", async () => {
    async function* chunks() {
      yield new Uint8Array([1, 2]);
      yield Buffer.from([3, 4, 5]);
    }
    expect(await collectRequestBody(chunks(), 5)).toEqual(Buffer.from([1, 2, 3, 4, 5]));
  });

  it("stops consuming as soon as the byte limit is exceeded", async () => {
    let yielded = 0;
    async function* chunks() {
      yielded++;
      yield new Uint8Array(4);
      yielded++;
      yield new Uint8Array(4);
      yielded++;
      yield new Uint8Array(4);
    }

    try {
      await collectRequestBody(chunks(), 6);
      throw new Error("expected ApiError");
    } catch (error) {
      expect(error).toBeInstanceOf(ApiError);
      expect((error as ApiError).status).toBe(413);
      expect((error as ApiError).options.code).toBe("request_too_large");
    }
    expect(yielded).toBe(2);
  });

  it("supports unbounded collection for existing Responses requests", async () => {
    async function* chunks() {
      yield new Uint8Array(8);
      yield new Uint8Array(8);
    }
    expect((await collectRequestBody(chunks(), Number.POSITIVE_INFINITY)).length).toBe(16);
  });

  it("detects oversized declared content lengths without trusting malformed values", () => {
    expect(contentLengthExceeds("101", 100)).toBe(true);
    expect(contentLengthExceeds("100", 100)).toBe(false);
    expect(contentLengthExceeds(undefined, 100)).toBe(false);
    expect(contentLengthExceeds("not-a-number", 100)).toBe(false);
    expect(declaredContentLength("100")).toBe(100);
    expect(declaredContentLength("not-a-number")).toBeNull();
    expect(declaredContentLength(String(Number.MAX_SAFE_INTEGER + 1))).toBeNull();
  });

  it("grows accounting before retaining each unknown-length chunk", async () => {
    const retained: number[] = [];
    async function* chunks() {
      yield Buffer.alloc(3);
      yield Buffer.alloc(5);
    }
    const body = await collectRequestBody(chunks(), 8, {
      beforeRetain(bytes) { retained.push(bytes); },
    });
    expect(retained).toEqual([3, 8]);
    expect(body.byteLength).toBe(8);
  });

  it("interrupts a pending body read with the shared request signal", async () => {
    const controller = new AbortController();
    const reason = new Error("absolute request deadline");
    async function* chunks() {
      yield Buffer.alloc(1);
      await new Promise(() => undefined);
    }
    const collecting = collectRequestBody(chunks(), 8, { signal: controller.signal });
    await Promise.resolve();
    controller.abort(reason);
    await expect(collecting).rejects.toBe(reason);
  });
});
