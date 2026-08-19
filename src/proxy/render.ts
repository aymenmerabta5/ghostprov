import { queueRender } from "../utils/queue";

export async function triggerRender(): Promise<void> {
  return queueRender(async () => {
    const tok = process.env.PRISM_SANDBOX_TOKEN ?? "mock-token";
    const bust = Math.floor(Math.random() * 1e15);
    const r = await fetch(
      `https://prism.openai.com/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust}`,
      {
        method: "POST",
        headers: { "x-crixet-sandbox-token": tok, "content-type": "application/json" },
        body: JSON.stringify({
          mainDocument: "main.tex",
          clientStateVector: "AerAuOEF8wI=",
          clientDeleteSetUpdate: "AAA=",
        }),
      },
    );
    if (r.status !== 202) throw new Error(`render ${r.status}`);
    for (let i = 0; i < 20; i++) {
      await new Promise((res) => setTimeout(res, 1000));
      const s = await fetch(
        `https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`,
        { headers: { "x-crixet-sandbox-token": tok } },
      ).then((x) => x.json() as Promise<{ status: string }>);
      if (s.status === "complete") return;
      if (s.status === "failed") throw new Error("render failed");
    }
    throw new Error("render timeout");
  });
}
