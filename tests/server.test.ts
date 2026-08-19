import { describe, it, expect } from "vitest";
import app from "../src/server";

describe("health", () => {
  it("GET /health returns ok", async () => {
    const res = await app.request("/health");
    expect(res.status).toBe(200);
    const j = await res.json();
    expect(j.ok).toBe(true);
  });
});
