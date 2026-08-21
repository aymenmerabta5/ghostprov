import { beforeEach, describe, expect, it, vi } from "bun:test";

const upload = vi.fn();
const content = vi.fn();

vi.mock("../src/proxy/files", () => ({
  uploadFileFromFormData: (...args: any[]) => upload(...args),
  getFileContent: (...args: any[]) => content(...args),
}));
vi.mock("../src/bridge/playwright", () => ({ startPlaywrightBridge: vi.fn() }));

import { app } from "../src/server";

beforeEach(() => {
  upload.mockReset();
  content.mockReset();
});

describe("active file routes", () => {
  it("forwards multipart upload data and returns the provider object", async () => {
    upload.mockImplementationOnce(async (form: FormData) => {
      const file = form.get("file") as File;
      expect(file.name).toBe("hello.txt");
      expect(await file.text()).toBe("hello");
      expect(form.get("purpose")).toBe("assistants");
      return { id: "file_1", object: "file", filename: file.name, purpose: "assistants" };
    });
    const form = new FormData();
    form.append("file", new File(["hello"], "hello.txt", { type: "text/plain" }));
    form.append("purpose", "assistants");
    const response = await app.request("/v1/files", { method: "POST", body: form });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ id: "file_1", filename: "hello.txt" });
  });

  it("returns exact file bytes from the content route", async () => {
    content.mockResolvedValueOnce(new TextEncoder().encode("payload"));
    const response = await app.request("/v1/files/file_1/content");
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/octet-stream");
    expect(await response.text()).toBe("payload");
    expect(content).toHaveBeenCalledWith("file_1");
  });

  it("maps missing provider content to 404", async () => {
    content.mockRejectedValueOnce(new Error("missing"));
    const response = await app.request("/v1/files/missing/content");
    expect(response.status).toBe(404);
  });
});
