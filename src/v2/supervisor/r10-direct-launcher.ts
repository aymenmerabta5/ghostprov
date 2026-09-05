import { V2_R10_RELEASE } from "../sidecar/assets";
import { supervisorInstallScript, type SupervisorExecRequest } from "./r10-launcher";

export const DIRECT_SUPERVISOR_LAUNCH_PATH = "exec";
export const DIRECT_SUPERVISOR_INSTALL_PATH = ".ghostprov-v2/direct-exec/install.sh";
const MAX_LAUNCH_BYTES = 12 * 1024;
const LEGACY_WORKSPACE_LINE = "WORKSPACE=$(readlink -f /workspace)";
const R10_ENTRYPOINT = `.ghostprov-v2/revisions/r10/${V2_R10_RELEASE.schemaHash}/${V2_R10_RELEASE.buildHash}/sidecar.bundle.js`;

export function isRegisteredR10Entrypoint(value: string): boolean { return value === R10_ENTRYPOINT; }

export interface DirectSupervisorLaunchFiles {
  readonly launcher: string;
  readonly installer: string;
}

/** Host-only adaptation; the registered R10 installer and bundle stay frozen. */
export function directSupervisorLaunchFiles(input: SupervisorExecRequest): DirectSupervisorLaunchFiles {
  if (input.entrypointPath !== R10_ENTRYPOINT) throw new TypeError("Direct supervisor requires the registered R10 entrypoint");
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/.test(input.supervisorNonce)) {
    throw new TypeError("Direct supervisor nonce is invalid");
  }
  const original = supervisorInstallScript(input);
  if (original.split(LEGACY_WORKSPACE_LINE).length !== 2) throw new TypeError("R10 installer workspace contract changed");
  const installer = original.replace(LEGACY_WORKSPACE_LINE, "WORKSPACE=$(pwd -P)");
  const launcher = [
    "#!/bin/sh",
    "set -eu",
    "WORKSPACE=$(pwd -P)",
    // The private execution session is supplied to the authenticated API,
    // not staged in Yjs beside the encrypted bootstrap credential.
    "export CODEX_THREAD_ID=${WORKSPACE##*/}",
    // Recovery uses the retained R10 supervisor. A new direct launch must not
    // retire a process belonging to a different execution session.
    "if [ -e /tmp/ghostprov-v2/supervisor/sidecar.pid ]; then exit 73; fi",
    `exec sh ${DIRECT_SUPERVISOR_INSTALL_PATH} >/dev/null 2>&1`,
    "",
  ].join("\n");
  if (new TextEncoder().encode(launcher).byteLength + new TextEncoder().encode(installer).byteLength > MAX_LAUNCH_BYTES) {
    throw new RangeError("Direct supervisor launcher exceeds its byte bound");
  }
  return Object.freeze({ launcher, installer });
}
