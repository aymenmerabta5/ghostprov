import { CapacityProofFailure, runStartupArtifactDiagnostic } from "./live_capacity_reliability";

if (import.meta.main) runStartupArtifactDiagnostic().catch(error => {
  console.error(JSON.stringify({ event: "startup.artifact.fatal", code: error instanceof CapacityProofFailure ? error.code : "external_operation_failed" }));
  process.exitCode = 1;
});
