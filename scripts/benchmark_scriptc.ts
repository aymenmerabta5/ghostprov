if (process.env.GP_BENCHMARK_SCRIPTC !== "1") {
  throw new Error("Set GP_BENCHMARK_SCRIPTC=1 to run the optional ScriptC gate");
}

const processResult = Bun.spawn([
  "npx",
  "--yes",
  "scriptc@0.0.35",
  "coverage",
  "src/server.ts",
  "--dynamic",
], { stdout: "inherit", stderr: "inherit", stdin: "inherit" });
const exitCode = await processResult.exited;
if (exitCode !== 0) process.exit(exitCode);

export {};
