import { describe, expect, it, vi } from "vitest";
import * as path from "node:path";
import { launchBridgeContext, resolveBridgeConfig } from "../src/bridge/config";

describe("bridge configuration", () => {
  it("derives portable defaults from the repository root and configured document", () => {
    const root = path.resolve("C:/work/ghostprov");
    const config = resolveBridgeConfig({ PRISM_DOC_ID: "doc-123" }, root);
    expect(config.userDataDir).toBe(path.join(root, ".pw"));
    expect(config.tokenFile).toBe(path.join(root, ".pw", "tokens.json"));
    expect(config.envFile).toBe(path.join(root, ".env"));
    expect(config.docId).toBe("doc-123");
    expect(config.prismUrl).toContain("u=doc-123");
  });

  it("honors explicit paths and project URL", () => {
    const config = resolveBridgeConfig({
      PRISM_DOC_ID: "configured-doc",
      PRISM_PROJECT_URL: "https://prism.openai.com/custom",
      GP_USER_DATA_DIR: "D:/profiles/ghostprov",
      GP_TOKEN_FILE: "D:/tokens/ghostprov.json",
      GP_ENV_FILE: "D:/config/ghostprov.env",
    }, "D:/repo");
    expect(config).toMatchObject({
      docId: "configured-doc",
      prismUrl: "https://prism.openai.com/custom",
      userDataDir: path.resolve("D:/profiles/ghostprov"),
      tokenFile: path.resolve("D:/tokens/ghostprov.json"),
      envFile: path.resolve("D:/config/ghostprov.env"),
    });
  });

  it("launches one persistent profile in both modes without unsafe arguments", async () => {
    const context = { close: vi.fn() };
    const chromium = {
      launch: vi.fn(),
      launchPersistentContext: vi.fn(async () => context),
    };
    const config = resolveBridgeConfig({ PRISM_DOC_ID: "doc" }, "D:/repo");
    await expect(launchBridgeContext(chromium, config, true)).resolves.toBe(context);
    await expect(launchBridgeContext(chromium, config, false)).resolves.toBe(context);
    expect(chromium.launch).not.toHaveBeenCalled();
    expect(chromium.launchPersistentContext).toHaveBeenNthCalledWith(1, config.userDataDir, expect.objectContaining({
      headless: true,
      args: expect.not.arrayContaining(["--no-sandbox"]),
    }));
    expect(chromium.launchPersistentContext).toHaveBeenNthCalledWith(2, config.userDataDir, expect.objectContaining({
      headless: false,
      args: expect.not.arrayContaining(["--no-sandbox"]),
    }));
  });
});
