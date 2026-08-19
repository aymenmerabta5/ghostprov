import { unzipSync } from "fflate";

export async function fetchZip(): Promise<Map<string, Uint8Array>> {
  const tok = process.env.PRISM_SANDBOX_TOKEN ?? "mock-token";
  const bust = Math.floor(Math.random() * 1e15);
  const r = await fetch(
    `https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${bust}`,
    { headers: { "x-crixet-sandbox-token": tok } },
  );
  if (!r.ok) {
    throw new Error(`fetchZip ${r.status} ${r.statusText}`);
  }
  const buf = new Uint8Array(await r.arrayBuffer());
  const unzipped = unzipSync(buf);
  return new Map(Object.entries(unzipped));
}

export async function extractResponse(): Promise<unknown> {
  const files = await fetchZip();
  const data = files.get("response.json");
  if (!data) throw new Error("response.json not found in zip");
  return JSON.parse(new TextDecoder().decode(data));
}
