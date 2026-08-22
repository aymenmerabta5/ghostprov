import { describe, expect, it } from "bun:test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import * as path from "node:path";

const root = path.resolve(import.meta.dirname, "..");

function sourceFiles(directory: string): string[] {
  const output: string[] = [];
  for (const name of readdirSync(directory)) {
    const file = path.join(directory, name);
    if (statSync(file).isDirectory()) output.push(...sourceFiles(file));
    else if (file.endsWith(".ts")) output.push(file);
  }
  return output;
}

describe("browserless production source", () => {
  it("contains no Playwright runtime dependency", () => {
    const pkg = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
    expect(pkg.dependencies).not.toHaveProperty("playwright");
    expect(pkg.devDependencies).not.toHaveProperty("playwright");
  });

  it("contains no production bridge or Playwright import", () => {
    const offenders = sourceFiles(path.join(root, "src"))
      .filter(file => /bridge[\\/]playwright|from ["']playwright["']|import\(["']playwright|playwright-core/.test(readFileSync(file, "utf8")))
      .map(file => path.relative(root, file));
    expect(offenders).toEqual([]);
  });

  it("removes the browser bridge entrypoints", () => {
    expect(existsSync(path.join(root, "src", "bridge", "playwright.ts"))).toBe(false);
    expect(existsSync(path.join(root, "src", "bridge", "config.ts"))).toBe(false);
  });
});
