/** render-cancel + render-status probe via server gateway. */
import "dotenv/config";
import { pageFetch } from "../src/prism/gateway";

const bust = () => `gp${Date.now().toString(36)}`;
const r1 = await pageFetch(`/s/sandboxes/proxy/render-cancel?prism_cache_bust=${bust()}`, { method: "POST", json: {} });
console.log("cancel:", r1.status, r1.text.slice(0, 200));
const r2 = await pageFetch(`/s/sandboxes/proxy/render-status?waitMs=2000&renderStatusMode=json&prism_cache_bust=${bust()}`);
console.log("status:", r2.status, r2.text.slice(0, 300));
process.exit(0);
