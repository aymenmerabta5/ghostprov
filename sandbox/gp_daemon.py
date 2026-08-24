# gp_daemon.py — ghostprov slot daemon (sandbox side)
#
# Watches legacy/v6.2 job_s<N>.json cursors plus immutable v6.3 commit parts
# under gp/. Each complete job is picked up by its own worker thread and
# streamed from the relay. A v6.3 client writes parts 1..K-1 first, then commits
# by writing part zero as:
#
#   GP63 {"v":6.3,"id":...,"parts":K,"size":T,"sha12":X,"enc":"gz+b64"}\n<payload0>
#
# No v6.3 request rewrites job_s<N>.json.
# ($OPENAI_BASE_URL = https://proxy:18080/v1). Progress lands in
# resp_s<N>.json using the v3 tail protocol so per-poll cost stays O(window):
#
#   progress: {"streaming":true,"ack":"<sha12 of job text>","total":N,
#              "start":S,"tail":"<window>","frames":k}
#   terminal: {"gp_final":<Responses object>,"gp_ack":...,"gp_sse":<full SSE>,
#              "gp_total":N,"gp_frames":k}, or a failed status object.
#
# The `ack` lets the client prove a resp file belongs to the job it wrote
# (stale-output protection across runs).
#
# Liveness: gp_status.json heartbeat {"pid","ts","active":{...},"done","failed"}
# every ~2s. Client treats status stale (>15s) as daemon-dead -> one render
# relaunches it via .latexmkrc.
#
# Concurrency: one thread per active slot (unbounded up to slot count);
# upstream connections are independent — no shared render queue.

import base64
import errno
import fcntl
import gzip
import hashlib
import json
import os
import pathlib
import re
import signal
import socket
import ssl
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request

SOURCE_PATH = pathlib.Path(__file__).resolve()
# The render sandbox mounts a project at /workspace, while Codex itself sees
# the same project under /codex_workspace/<id>. Anchor the default to the file
# being executed so both launch paths watch their own adjacent lane files.
WORKSPACE = pathlib.Path(os.environ.get("WORKSPACE_DIR", str(SOURCE_PATH.parent)))
NUM_SLOTS = int(os.environ.get("GP_SLOTS", "30"))
GENERATION = int(os.environ.get("GP_GENERATION", "0"))
PIDFILE = pathlib.Path(os.environ.get("GP_PIDFILE", str(WORKSPACE / ".gp_daemon.pid")))
POLL_INTERVAL = 0.25
STATUS_EVERY = 2.0
WINDOW = 64 * 1024
PROGRESS_EVERY = 2048
FINAL_FLUSH_GRACE = 0.1
UPSTREAM_IDLE_TIMEOUT = float(os.environ.get("GP_HTTP_TIMEOUT", "120"))
MAX_SEGMENTED_JOB_BYTES = int(
    os.environ.get("GP_MAX_JOB_BYTES", str(64 * 1024 * 1024))
)
MAX_JOB_PART_BYTES = int(
    os.environ.get("GP_MAX_JOB_PART_BYTES", str(256 * 1024))
)
MAX_JOB_PARTS = int(os.environ.get("GP_MAX_JOB_PARTS", "4096"))
SEGMENT_REQUEST_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SHA12_PATTERN = re.compile(r"^[0-9a-f]{12}$")
V63_MAGIC = b"GP63 "
MAX_V63_HEADER_BYTES = 1024
V63_PART0_PATTERN = re.compile(
    r"^job_s([1-9][0-9]*)\.json\.([A-Za-z0-9_-]{1,128})\.0$"
)
V63_SCAN_INTERVAL = float(os.environ.get("GP_V63_SCAN_INTERVAL", "0.5"))
V63_RECENT_TTL = float(os.environ.get("GP_V63_RECENT_TTL", "300"))
V63_RECENT_MAX = int(os.environ.get("GP_V63_RECENT_MAX", "512"))
DAEMON_BUILD = "v6.3.1-bounded"

_lock = threading.Lock()
_active = {}          # slot -> ack currently processing (legacy status field)
_active_requests = {} # slot -> request identity/status metadata
_assembling_requests = {} # slot -> v6.2 manifest awaiting immutable parts
_done = 0
_failed = 0
_last_status_write = 0.0
_detach_mode = "not_attempted"


def sha12(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def sha12_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()[:12]


class SegmentManifestError(ValueError):
    pass


class SegmentAssemblyError(ValueError):
    pass


def _detach_from_launcher():
    """Move the daemon out of the render launcher's process/session group.

    Prism may reap a completed render's process group. `nohup` only changes
    signal disposition and output; it does not create a new session. Detaching
    here keeps the daemon safe even if the launcher file is echoed back to an
    older `nohup ... &` version by document convergence.
    """
    global _detach_mode
    try:
        os.setsid()
        _detach_mode = "setsid"
        return
    except OSError as ex:
        if ex.errno != errno.EPERM:
            raise

    if os.getsid(0) == os.getpid():
        _detach_mode = "already_session_leader"
        return

    # Rare case: the launcher made us a process-group leader but not a session
    # leader. Fork once so the child can call setsid safely; no threads or locks
    # exist yet because this runs at the first line of main().
    child = os.fork()
    if child:
        os._exit(0)
    os.setsid()
    _detach_mode = "fork_setsid"


def _termination_signal(signum, _frame):
    try:
        name = signal.Signals(signum).name
    except Exception:
        name = str(signum)
    print(
        f"[gp_daemon] terminating signal={name} pid={os.getpid()} "
        f"ppid={os.getppid()} sid={os.getsid(0)} pgid={os.getpgrp()}",
        flush=True,
    )
    raise SystemExit(128 + signum)


def _install_signal_diagnostics():
    for signum in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(signum, _termination_signal)


def _bounded_string(value, limit=200):
    return value[:limit] if isinstance(value, str) and value else None


def request_identity(job_text: str):
    """Extract stable Codex-agent identity without conflating it with a slot.

    A Codex agent can issue many sequential Responses API requests, and each
    request may use a different transport slot.  The thread/agent metadata in
    the request is therefore the durable identity; the slot is only a lane.
    """
    try:
        body = json.loads(job_text)
    except Exception:
        return {}
    if not isinstance(body, dict):
        return {}

    client = body.get("client_metadata")
    if not isinstance(client, dict):
        client = {}
    turn = client.get("x-codex-turn-metadata")
    if isinstance(turn, str):
        try:
            turn = json.loads(turn)
        except Exception:
            turn = {}
    if not isinstance(turn, dict):
        turn = {}

    identity = {}
    for output_key, metadata_key in (
        ("agent_name", "agent_name"),
        ("thread_id", "thread_id"),
        ("session_id", "session_id"),
        ("turn_id", "turn_id"),
        ("root_turn_id", "root_turn_id"),
        ("request_kind", "request_kind"),
        ("thread_source", "thread_source"),
    ):
        value = _bounded_string(turn.get(metadata_key) or client.get(metadata_key))
        if value is not None:
            identity[output_key] = value
    cache_key = _bounded_string(body.get("prompt_cache_key"))
    if cache_key is not None:
        identity["prompt_cache_key"] = cache_key

    # thread_id is the durable Codex-agent identity. prompt_cache_key/session_id
    # identify a wider cache family and may be shared by the root and subagents.
    agent_id = identity.get("thread_id")
    if agent_id is not None:
        identity["agent_id"] = agent_id
    family_id = cache_key or identity.get("session_id") or agent_id
    if family_id is not None:
        identity["family_id"] = family_id
    return identity


def consume_generation_fence(body):
    """Remove the private generation field and prove this daemon owns the job."""
    if not isinstance(body, dict):
        return False
    return body.pop("_ghostprov_generation", 0) == GENERATION


def ensure_slot_files():
    """Create all lane endpoints without changing existing jobs or responses."""
    (WORKSPACE / "gp").mkdir(parents=True, exist_ok=True)
    for slot in range(1, NUM_SLOTS + 1):
        for prefix in ("job", "resp"):
            path = WORKSPACE / f"{prefix}_s{slot}.json"
            try:
                path.open("x").close()
            except FileExistsError:
                pass


def _normalize_segment_manifest(value: dict, version: float):
    request_id = value.get("id")
    parts = value.get("parts")
    size = value.get("size")
    ack = value.get("sha12")
    encoding = value.get("enc")
    if not isinstance(request_id, str) or not SEGMENT_REQUEST_ID.fullmatch(request_id):
        raise SegmentManifestError("v6 manifest id must be 1-128 safe filename characters")
    if isinstance(parts, bool) or not isinstance(parts, int) or not (1 <= parts <= MAX_JOB_PARTS):
        raise SegmentManifestError(f"v6 manifest parts must be between 1 and {MAX_JOB_PARTS}")
    if isinstance(size, bool) or not isinstance(size, int) or not (1 <= size <= MAX_SEGMENTED_JOB_BYTES):
        raise SegmentManifestError(
            f"v6 manifest size must be between 1 and {MAX_SEGMENTED_JOB_BYTES} decoded bytes"
        )
    if not isinstance(ack, str) or not SHA12_PATTERN.fullmatch(ack):
        raise SegmentManifestError("v6 manifest sha12 must be 12 lowercase hexadecimal characters")
    if encoding is not None and encoding != "gz+b64":
        raise SegmentManifestError(f"unsupported v6 encoding {encoding!r}")
    return {
        "v": version,
        "id": request_id,
        "parts": parts,
        "size": size,
        "sha12": ack,
        "encoding": encoding,
    }


def parse_segment_manifest(job_text: str):
    """Return a normalized v6.2 cursor manifest, or None for a legacy job."""
    try:
        value = json.loads(job_text)
    except Exception:
        return None
    if not isinstance(value, dict):
        return None
    if str(value.get("v")) not in ("6", "6.0", "6.2"):
        return None
    return _normalize_segment_manifest(value, 6.2)


def parse_v63_part0(value: bytes, expected_id: str | None = None):
    """Parse a write-once v6.3 commit header and return (manifest, payload).

    Wire format for canonical part zero:

        GP63 {compact manifest JSON}\n<first encoded payload slice>

    Parts 1..K-1 remain unchanged. Clients publish part zero last; its creation
    commits the job without rewriting the churn-prone job_sN.json cursor.
    """
    if not value.startswith(V63_MAGIC):
        return None
    newline = value.find(b"\n", len(V63_MAGIC))
    if newline < 0 or newline - len(V63_MAGIC) > MAX_V63_HEADER_BYTES:
        raise SegmentManifestError("v6.3 part 0 has a missing or oversized header")
    try:
        header = json.loads(value[len(V63_MAGIC):newline].decode("utf-8"))
    except Exception as ex:
        raise SegmentManifestError(f"v6.3 part 0 header is invalid JSON: {ex}") from ex
    if not isinstance(header, dict) or str(header.get("v")) != "6.3":
        raise SegmentManifestError("v6.3 part 0 header must declare v=6.3")
    manifest = _normalize_segment_manifest(header, 6.3)
    if expected_id is not None and manifest["id"] != expected_id:
        raise SegmentManifestError(
            f"v6.3 filename id {expected_id!r} does not match header id {manifest['id']!r}"
        )
    manifest["embedded_header"] = True
    return manifest, value[newline + 1:]


def format_v63_part0(manifest: dict, first_payload: bytes) -> bytes:
    """Build canonical part zero; shared by tests and client implementers."""
    header = {
        "v": 6.3,
        "id": manifest["id"],
        "parts": manifest["parts"],
        "size": manifest["size"],
        "sha12": manifest["sha12"],
    }
    encoding = manifest.get("encoding") or manifest.get("enc")
    if encoding is not None:
        header["enc"] = encoding
    normalized = _normalize_segment_manifest(header, 6.3)
    encoded = json.dumps(header, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_V63_HEADER_BYTES:
        raise SegmentManifestError("v6.3 header exceeds maximum size")
    # Keep validation and the emitted dictionary in lockstep.
    assert normalized["id"] == manifest["id"]
    return V63_MAGIC + encoded + b"\n" + first_payload


def segmented_part_path(slot: int, request_id: str, index: int) -> pathlib.Path:
    return WORKSPACE / "gp" / f"job_s{slot}.json.{request_id}.{index}"


def legacy_segmented_part_path(slot: int, request_id: str, index: int) -> pathlib.Path:
    return WORKSPACE / "gp" / f"job_s{slot}.{request_id}.{index}"


def resolve_segmented_part_path(slot: int, request_id: str, index: int) -> pathlib.Path:
    canonical = segmented_part_path(slot, request_id, index)
    return canonical if canonical.exists() else legacy_segmented_part_path(slot, request_id, index)


def cleanup_v63_parts(slot: int, request_id: str):
    """Delete every canonical/rollout part belonging to one processed request."""
    if not SEGMENT_REQUEST_ID.fullmatch(request_id):
        return []
    gp_dir = WORKSPACE / "gp"
    paths = list(gp_dir.glob(f"job_s{slot}.json.{request_id}.*"))
    paths.extend(gp_dir.glob(f"job_s{slot}.{request_id}.*"))
    removed = []
    for path in paths:
        try:
            path.unlink()
            removed.append(path)
        except FileNotFoundError:
            pass
        except OSError as ex:
            print(f"[gp_daemon] cleanup failed path={path}: {ex}", flush=True)
    return removed


def mark_v63_seen(seen: dict, key, now=None):
    seen[key] = time.monotonic() if now is None else now


def prune_v63_metadata(known: dict, seen: dict, now=None):
    """Bound recent request IDs and discard scan entries for deleted files."""
    current = time.monotonic() if now is None else now
    for path in list(known):
        if not pathlib.Path(path).exists():
            known.pop(path, None)
    cutoff = current - V63_RECENT_TTL
    for key, timestamp in list(seen.items()):
        if timestamp < cutoff:
            seen.pop(key, None)
    overflow = len(seen) - V63_RECENT_MAX
    if overflow > 0:
        for key, _ in sorted(seen.items(), key=lambda item: item[1])[:overflow]:
            seen.pop(key, None)


def retire_v63_event(event: dict, known: dict, seen: dict, now=None):
    removed = cleanup_v63_parts(event["slot"], event["request_id"])
    for path in removed:
        known.pop(str(path), None)
    mark_v63_seen(seen, event["key"], now=now)


def publish_v63_failure_if_idle(event: dict, worker, known: dict, seen: dict, now=None):
    """Publish a malformed-commit terminal only after the lane worker exits."""
    if worker is not None and worker.is_alive():
        return False
    write_slot_failed(
        event["slot"],
        event["ack"],
        event["code"],
        event["message"],
        transport=event["transport"],
    )
    retire_v63_event(event, known, seen, now=now)
    return True


def read_bytes_snapshot(path: pathlib.Path):
    for _ in range(3):
        try:
            before = path.stat()
            value = path.read_bytes()
            after = path.stat()
        except (FileNotFoundError, OSError):
            return None
        if (before.st_mtime_ns, before.st_size) == (after.st_mtime_ns, after.st_size):
            return value
        time.sleep(0.01)
    return None


def assemble_segmented_job(slot: int, manifest: dict):
    """Assemble one lane independently; None means immutable parts are pending."""
    paths = [
        resolve_segmented_part_path(slot, manifest["id"], index)
        for index in range(manifest["parts"])
    ]
    try:
        stats = [path.stat() for path in paths]
    except (FileNotFoundError, OSError):
        return None
    for index, stat in enumerate(stats):
        if stat.st_size > MAX_JOB_PART_BYTES:
            raise SegmentAssemblyError(
                f"v6 part {index} is {stat.st_size} bytes; maximum is {MAX_JOB_PART_BYTES}"
            )

    parts = []
    for path in paths:
        value = read_bytes_snapshot(path)
        if value is None:
            return None
        parts.append(value)
    if manifest.get("embedded_header"):
        parsed = parse_v63_part0(parts[0], expected_id=manifest["id"])
        if parsed is None:
            raise SegmentAssemblyError("v6.3 part 0 lost its embedded commit header")
        header_manifest, first_payload = parsed
        for key in ("parts", "size", "sha12", "encoding"):
            if header_manifest.get(key) != manifest.get(key):
                raise SegmentAssemblyError(
                    f"v6.3 part 0 header changed field {key!r} during assembly"
                )
        parts[0] = first_payload
    raw = b"".join(parts)
    if manifest.get("encoding") == "gz+b64":
        try:
            raw = gzip.decompress(base64.b64decode(raw, validate=True))
        except Exception as ex:
            raise SegmentAssemblyError(f"v6 gunzip failed: {ex}") from ex
    if len(raw) != manifest["size"]:
        raise SegmentAssemblyError(
            f"v6 assembled size {len(raw)} does not match manifest size {manifest['size']}"
        )
    actual_ack = sha12_bytes(raw)
    if actual_ack != manifest["sha12"]:
        raise SegmentAssemblyError(
            f"v6 assembled sha12 {actual_ack} does not match manifest sha12 {manifest['sha12']}"
        )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as ex:
        raise SegmentAssemblyError(f"v6 assembled job is not valid UTF-8: {ex}") from ex


def _source_signature():
    # Yjs/doc-to-disk convergence can replace this file with byte-identical
    # content, changing mtime and sometimes inode metadata. Hot reload must be
    # content-addressed or those harmless echoes become an exec restart loop.
    value = SOURCE_PATH.read_bytes()
    return len(value), hashlib.sha256(value).digest()


def response_is_terminal_for_ack(
    path: pathlib.Path,
    ack: str,
    job_mtime_ns: int | None = None,
    request_id: str | None = None,
) -> bool:
    """Return whether `path` proves that `ack` already reached a terminal state.

    Accepted/progress/final-flush frames are deliberately non-terminal here.
    If the daemon died after writing one of those frames, the request must be
    recovered on boot rather than swallowed by the initial signature scan.
    """
    try:
        response_mtime_ns = path.stat().st_mtime_ns
        value = json.loads(path.read_text())
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return False
    if not isinstance(value, dict):
        return False
    completed = value.get("gp_ack") == ack and isinstance(value.get("gp_final"), dict)
    failed = value.get("ack") == ack and value.get("status") == "failed"
    if not (completed or failed):
        return False
    if request_id is not None:
        transport = value.get("gp_transport") if completed else value.get("transport")
        if isinstance(transport, dict):
            return transport.get("id") == request_id
        # Rollout compatibility: v6.2.2 terminal frames did not include the
        # manifest id. Their mtime still safely proves completion unless the
        # client rewrote the manifest afterward.
        return job_mtime_ns is not None and response_mtime_ns >= job_mtime_ns
    # Legacy jobs have no immutable request id. A same-content job rewritten
    # after the response is an intentional new generation and must recover.
    if job_mtime_ns is not None and response_mtime_ns < job_mtime_ns:
        return False
    return True


def _atomic_write_json(path: pathlib.Path, value):
    """Replace a JSON file atomically so polling readers never see half a frame."""
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        tmp.write_text(json.dumps(value))
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def manifest_ack_hint(job_text: str) -> str:
    try:
        value = json.loads(job_text)
        ack = value.get("sha12") if isinstance(value, dict) else None
    except Exception:
        ack = None
    return ack if isinstance(ack, str) and SHA12_PATTERN.fullmatch(ack) else sha12(job_text)


def set_assembling(slot: int, manifest: dict):
    with _lock:
        _assembling_requests[slot] = {
            "v": manifest.get("v"),
            "id": manifest["id"],
            "ack": manifest["sha12"],
            "parts": manifest["parts"],
            "size": manifest["size"],
            "encoding": manifest.get("encoding"),
        }


def clear_assembling(slot: int):
    with _lock:
        _assembling_requests.pop(slot, None)


def write_slot_failed(
    slot: int,
    ack: str,
    code: str,
    message,
    request=None,
    transport=None,
):
    global _failed
    with _lock:
        _failed += 1
        _assembling_requests.pop(slot, None)
    try:
        value = {
            "status": "failed",
            "ack": ack,
            "slot": slot,
            "request": request or {},
            "error": {"code": code, "message": str(message)[:2000]},
        }
        if transport:
            value["transport"] = dict(transport)
        _atomic_write_json(WORKSPACE / f"resp_s{slot}.json", value)
    except Exception:
        pass


def _pidfile_guard():
    """Own the daemon pidfile lock for this process's lifetime.

    The launcher intentionally starts the daemon on every render.  An advisory
    lock is atomic, survives concurrent launch attempts, and is automatically
    released by the kernel if the owner exits.  The pidfile itself is left in
    place because unlinking a locked file can let two processes lock different
    inodes under the same name.
    """
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    PIDFILE.parent.mkdir(parents=True, exist_ok=True)
    handle = PIDFILE.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as ex:
        if ex.errno not in (errno.EACCES, errno.EAGAIN):
            handle.close()
            raise
        handle.seek(0)
        owner = handle.read().strip() or "unknown"
        print(f"[gp_daemon] already running (pid={owner}); exiting", flush=True)
        handle.close()
        return None

    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    os.fsync(handle.fileno())
    return handle


def write_status():
    global _done, _failed, _last_status_write
    with _lock:
        active = dict(_active)
        active_requests = {
            slot: dict(request) for slot, request in _active_requests.items()
        }
        assembling_requests = {
            slot: dict(request) for slot, request in _assembling_requests.items()
        }
        active_agents = {}
        active_families = {}
        for slot, request in active_requests.items():
            agent_id = (
                request.get("agent_id")
                or request.get("thread_id")
                or f"anonymous:slot-{slot}"
            )
            group = active_agents.setdefault(agent_id, {
                "agent_name": request.get("agent_name"),
                "thread_id": request.get("thread_id"),
                "slots": [],
                "request_count": 0,
            })
            group["slots"].append(slot)
            group["request_count"] += 1
            family_id = request.get("family_id") or request.get("session_id")
            if family_id is not None:
                family = active_families.setdefault(
                    family_id,
                    {"slots": [], "request_count": 0, "agent_ids": set()},
                )
                family["slots"].append(slot)
                family["request_count"] += 1
                family["agent_ids"].add(agent_id)
        for group in active_agents.values():
            group["slots"].sort()
        for family in active_families.values():
            family["slots"].sort()
            family["agent_ids"] = sorted(family["agent_ids"])
            family["agent_count"] = len(family["agent_ids"])
        payload = {
            "version": 5,
            "protocol": "tail-v3-terminal-sse",
            "daemon_build": DAEMON_BUILD,
            "generation": GENERATION,
            "job_protocols": [
                "single-v5",
                "segmented-v6.2",
                "segmented-v6.2-gz+b64",
                "segmented-v6.3-embedded-commit",
                "segmented-v6.3.1-bounded",
            ],
            "response_protocol": "single-v5-tail-terminal",
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "sid": os.getsid(0),
            "pgid": os.getpgrp(),
            "detach_mode": _detach_mode,
            "ts": time.time(),
            # `slots` remains for older clients.  The explicit names below
            # distinguish capacity/lanes from Codex agents and API requests.
            "slots": NUM_SLOTS,
            "slot_capacity": NUM_SLOTS,
            "active": active,
            "active_slot_count": len(active),
            "available_slot_count": max(0, NUM_SLOTS - len(active)),
            "active_requests": active_requests,
            "active_request_count": len(active_requests),
            "active_agents": active_agents,
            "active_agent_count": len(active_agents),
            "active_cache_families": active_families,
            "active_cache_family_count": len(active_families),
            "assembling_requests": assembling_requests,
            "assembling_request_count": len(assembling_requests),
            "done": _done,
            "failed": _failed,
            "completed_requests": _done,
            "failed_requests": _failed,
            "upstream_idle_timeout_seconds": UPSTREAM_IDLE_TIMEOUT,
        }
    try:
        _atomic_write_json(WORKSPACE / "gp_status.json", payload)
        _last_status_write = time.monotonic()
    except Exception:
        pass


def status_loop():
    while True:
        try:
            write_status()
        except Exception:
            traceback.print_exc()
        time.sleep(STATUS_EVERY)


class SlotWorker(threading.Thread):
    def __init__(
        self,
        slot: int,
        job_path: pathlib.Path,
        job_text: str,
        transport=None,
    ):
        super().__init__(daemon=True, name=f"gp-slot-{slot}")
        self.slot = slot
        self.job_path = job_path
        self.job_text = job_text
        self.ack = sha12(job_text or "")
        self.transport = dict(transport or {})
        self.resp_path = WORKSPACE / f"resp_s{slot}.json"
        self.request = request_identity(job_text)

    def progress(self, ack, total, start, tail, **extra):
        obj = {
            "streaming": True,
            "ack": ack,
            "slot": self.slot,
            "request": self.request,
            "total": total,
            "start": start,
            "tail": tail,
        }
        if self.transport:
            obj["transport"] = self.transport
        obj.update(extra)
        try:
            _atomic_write_json(self.resp_path, obj)
        except Exception:
            pass

    def write_failed(self, ack, code, message):
        write_slot_failed(
            self.slot,
            ack,
            code,
            message,
            request=self.request,
            transport=self.transport,
        )

    def run(self):
        global _done
        job_text = self.job_text
        ack = self.ack
        with _lock:
            _assembling_requests.pop(self.slot, None)
            _active[self.slot] = ack
            _active_requests[self.slot] = {
                "ack": ack,
                "started_at": time.time(),
                **self.request,
            }
        try:
            # FIRST WRITE: clear + ack frame — proves this resp belongs to this job
            self.progress(ack, 0, 0, "", phase="accepted")
            if not job_text:
                self.write_failed(ack, "no_job", "job file missing/unreadable")
                return
            if self.process(job_text, ack):
                with _lock:
                    _done += 1
        except (TimeoutError, socket.timeout) as ex:
            self.write_failed(
                ack,
                "upstream_timeout",
                f"upstream was idle for {UPSTREAM_IDLE_TIMEOUT:g}s: {ex}",
            )
        except urllib.error.URLError as ex:
            if isinstance(ex.reason, (TimeoutError, socket.timeout)):
                self.write_failed(
                    ack,
                    "upstream_timeout",
                    f"upstream was idle for {UPSTREAM_IDLE_TIMEOUT:g}s: {ex.reason}",
                )
            else:
                self.write_failed(ack, "connection_error", str(ex.reason))
        except Exception as ex:
            traceback.print_exc()
            self.write_failed(ack, "worker_exception", repr(ex)[:500])
        finally:
            with _lock:
                _active.pop(self.slot, None)
                _active_requests.pop(self.slot, None)

    def process(self, job_text: str, ack: str):
        try:
            body = json.loads(job_text)
        except Exception as ex:
            self.write_failed(ack, "bad_job_json", repr(ex)[:300])
            return False
        if not isinstance(body, dict):
            self.write_failed(ack, "bad_job_json", "job body is not an object")
            return False
        if not consume_generation_fence(body):
            # Another sandbox/runtime generation owns this immutable job.  Do
            # not write a response: the matching daemon will process it.
            return False
        body.setdefault("store", False)
        body.setdefault("service_tier", "fast")

        BASE = os.environ["OPENAI_BASE_URL"].rstrip("/")
        KEY = os.environ["OPENAI_API_KEY"]
        CA = os.environ.get("PROXY_CA_CERT_PATH")
        CTX = ssl.create_default_context(cafile=CA) if CA else None
        stream = bool(body.get("stream"))
        headers = {"Authorization": "Bearer " + KEY, "Content-Type": "application/json"}
        if stream:
            headers["Accept"] = "text/event-stream"
        req = urllib.request.Request(BASE + "/responses", data=json.dumps(body).encode(), headers=headers)
        try:
            r = urllib.request.urlopen(
                req,
                context=CTX,
                timeout=UPSTREAM_IDLE_TIMEOUT,
            )
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", "replace")[:1500]
            print(f"[slot {self.slot}] HTTPError {e.code} {err_body[:200]}", flush=True)
            if e.code == 429:
                # TPM is shared by the OpenAI organization/model, not by this
                # sandbox. Retrying here (or in another project) only hides the
                # canonical error and adds pressure to the same exhausted pool.
                self.write_failed(ack, "rate_limited_429", err_body)
                return False
            else:
                self.write_failed(ack, f"http_{e.code}", err_body)
                return False

        with r:
            if stream:
                acc = []
                acc_len = 0
                last_prog = 0
                frames = 0
                while True:
                    line = r.readline()
                    if not line:
                        break
                    txt = line.decode("utf-8", "replace")
                    acc.append(txt)
                    acc_len += len(txt)
                    frames += 1
                    if acc_len - last_prog >= PROGRESS_EVERY:
                        start = max(0, acc_len - WINDOW)
                        self.progress(ack, acc_len, start, "".join(acc)[start:], frames=frames)
                        last_prog = acc_len
                full = "".join(acc)
                final = None
                for ev in full.split("\n\n"):
                    for ln in ev.split("\n"):
                        if ln.startswith("data:") and "response.completed" in ln:
                            try:
                                final = json.loads(ln[5:].strip()).get("response")
                            except Exception:
                                pass
                if final is None:
                    print(f"[slot {self.slot}] stream truncated at {len(full)} bytes", flush=True)
                    self.write_failed(ack, "stream_truncated",
                                      "connection ended before response.completed; received %d bytes" % len(full))
                    return False
                # Publish every last delta before the terminal frame. Polling
                # clients may miss this short-lived snapshot, so the terminal
                # wrapper also carries the complete SSE transcript below.
                start = max(0, acc_len - WINDOW)
                self.progress(
                    ack,
                    acc_len,
                    start,
                    full[start:],
                    frames=frames,
                    phase="final_flush",
                    complete=True,
                )
                time.sleep(FINAL_FLUSH_GRACE)
                # wrap so the client can prove provenance (ack) while the inner
                # Responses object stays verbatim. gp_sse is authoritative for
                # replaying any events the client did not consume from progress.
                try:
                    value = {
                        "gp_final": final,
                        "gp_ack": ack,
                        "gp_slot": self.slot,
                        "gp_request": self.request,
                        "gp_sse": full,
                        "gp_total": acc_len,
                        "gp_frames": frames,
                    }
                    if self.transport:
                        value["gp_transport"] = self.transport
                    _atomic_write_json(self.resp_path, value)
                except Exception:
                    pass
            else:
                resp = json.load(r)
                try:
                    value = {
                        "gp_final": resp,
                        "gp_ack": ack,
                        "gp_slot": self.slot,
                        "gp_request": self.request,
                    }
                    if self.transport:
                        value["gp_transport"] = self.transport
                    _atomic_write_json(self.resp_path, value)
                except Exception:
                    pass
        return True


def scan_slots(known: dict):
    """Return list of (slot, path) whose content changed since last scan."""
    changed = []
    for slot in range(1, NUM_SLOTS + 1):
        p = WORKSPACE / f"job_s{slot}.json"
        try:
            st = p.stat()
            sig = (st.st_mtime_ns, st.st_size)
        except FileNotFoundError:
            known.pop(slot, None)
            continue
        except Exception:
            continue
        if known.get(slot) != sig:
            changed.append((slot, p))
    return changed


def read_job_snapshot(path: pathlib.Path):
    """Read a job only when its size and timestamp are stable around the read."""
    for _ in range(3):
        try:
            before = path.stat()
            text = path.read_text()
            after = path.stat()
        except (FileNotFoundError, OSError):
            return None
        before_sig = (before.st_mtime_ns, before.st_size)
        after_sig = (after.st_mtime_ns, after.st_size)
        if before_sig == after_sig:
            return after_sig, text
        time.sleep(0.01)
    return None


def _v63_ack_hint(value: bytes) -> str:
    try:
        newline = value.find(b"\n", len(V63_MAGIC))
        header = json.loads(value[len(V63_MAGIC):newline].decode("utf-8"))
        ack = header.get("sha12") if isinstance(header, dict) else None
    except Exception:
        ack = None
    return ack if isinstance(ack, str) and SHA12_PATTERN.fullmatch(ack) else sha12_bytes(value)


def scan_v63_commits(known: dict):
    """Discover changed write-once part-zero commits under gp/.

    Stable non-v6.3 and empty files are remembered so the scanner does not
    repeatedly read the accumulated v6.2/cleaned artifact set.
    """
    discovered = []
    failures = []
    gp_dir = WORKSPACE / "gp"
    try:
        paths = list(gp_dir.glob("job_s*.json.*.0"))
    except OSError:
        return discovered, failures
    live_paths = set()
    for path in paths:
        match = V63_PART0_PATTERN.fullmatch(path.name)
        if match is None:
            continue
        slot = int(match.group(1))
        request_id = match.group(2)
        if slot > NUM_SLOTS:
            continue
        key = str(path)
        live_paths.add(key)
        try:
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            continue
        if known.get(key) == signature:
            continue
        value = read_bytes_snapshot(path)
        if value is None:
            continue
        known[key] = signature
        if not value or not value.startswith(V63_MAGIC):
            continue
        try:
            parsed = parse_v63_part0(value, expected_id=request_id)
        except SegmentManifestError as ex:
            transport = {"v": 6.3, "id": request_id, "embedded_header": True}
            failures.append({
                "kind": "failure",
                "slot": slot,
                "key": (slot, request_id),
                "request_id": request_id,
                "order": (signature[0], request_id),
                "signature": signature,
                "commit_path": path,
                "ack": _v63_ack_hint(value),
                "code": "bad_v63_commit",
                "message": str(ex),
                "transport": transport,
            })
            continue
        manifest, _ = parsed
        discovered.append({
            "kind": "commit",
            "slot": slot,
            "key": (slot, request_id),
            "request_id": request_id,
            "order": (signature[0], request_id),
            "signature": signature,
            "commit_path": path,
            "manifest": manifest,
        })
    for key in set(known) - live_paths:
        known.pop(key, None)
    return discovered, failures


def select_newest_v63_events(events):
    """Return ({slot: newest}, losers) ordered by (mtime_ns, request_id)."""
    newest = {}
    losers = []
    for event in events:
        previous = newest.get(event["slot"])
        if previous is None or event["order"] > previous["order"]:
            if previous is not None:
                losers.append(previous)
            newest[event["slot"]] = event
        else:
            losers.append(event)
    return newest, losers


def scan_startup_slots(skip_slots=None):
    """Baseline slots and recover legacy or segmented jobs not proven terminal.

    The old startup path blindly marked every existing job as known.  A client
    write during the daemon restart gap was therefore never dispatched.  The
    response acknowledgement is the durable completion record: only a matching
    terminal response is safe to baseline without replaying the job.
    """
    skip_slots = set(skip_slots or ())
    known = {}
    ready = []
    assembling = {}
    failures = []
    for slot in range(1, NUM_SLOTS + 1):
        job_path = WORKSPACE / f"job_s{slot}.json"
        snapshot = read_job_snapshot(job_path)
        if snapshot is None:
            continue
        signature, job_text = snapshot
        known[slot] = signature
        if slot in skip_slots:
            continue
        if not job_text.strip():
            continue
        response_path = WORKSPACE / f"resp_s{slot}.json"
        try:
            manifest = parse_segment_manifest(job_text)
        except SegmentManifestError as ex:
            failures.append((slot, manifest_ack_hint(job_text), "bad_v6_manifest", str(ex)))
            continue
        if manifest is None:
            ack = sha12(job_text)
            if not response_is_terminal_for_ack(response_path, ack, signature[0]):
                ready.append((slot, job_path, job_text, None))
            continue

        ack = manifest["sha12"]
        if response_is_terminal_for_ack(
            response_path,
            ack,
            signature[0],
            manifest["id"],
        ):
            continue
        try:
            assembled = assemble_segmented_job(slot, manifest)
        except SegmentAssemblyError as ex:
            failures.append((slot, ack, "bad_v6_parts", str(ex)))
            continue
        if assembled is None:
            assembling[slot] = {
                "signature": signature,
                "job_path": job_path,
                "manifest": manifest,
            }
        else:
            ready.append((slot, job_path, assembled, manifest))
    return known, ready, assembling, failures


def main():
    _install_signal_diagnostics()
    _detach_from_launcher()
    # Keep this handle alive for the entire main loop.  Closing it releases the
    # flock, so concurrent launch attempts can safely try again after exit.
    pidfile_handle = _pidfile_guard()
    if pidfile_handle is None:
        return

    print(
        f"[gp_daemon] pid={os.getpid()} build={DAEMON_BUILD} slots={NUM_SLOTS} "
        f"workspace={WORKSPACE} ppid={os.getppid()} sid={os.getsid(0)} "
        f"pgid={os.getpgrp()} detach={_detach_mode}",
        flush=True,
    )
    ensure_slot_files()
    print(f"[gp_daemon] provisioned {NUM_SLOTS} transport lanes", flush=True)
    threading.Thread(target=status_loop, daemon=True, name="gp-status").start()

    v63_known = {}
    v63_queue = {}
    v63_seen = {}
    v63_inflight = {}
    next_v63_scan = 0.0
    initial_v63, initial_v63_failures = scan_v63_commits(v63_known)
    v63_queue, initial_losers = select_newest_v63_events(
        [*initial_v63, *initial_v63_failures]
    )
    for loser in initial_losers:
        cleanup_v63_parts(loser["slot"], loser["request_id"])
        mark_v63_seen(v63_seen, loser["key"])
    v63_startup_slots = set(v63_queue)
    known, startup_jobs, segmented_pending, startup_failures = scan_startup_slots(
        skip_slots=v63_startup_slots
    )
    workers: dict = {}
    source_signature = _source_signature()
    restart_pending = False
    for slot, ack, code, message in startup_failures:
        write_slot_failed(slot, ack, code, message)
        print(f"[gp_daemon] slot {slot} rejected at startup: {code}", flush=True)
    for event in v63_queue.values():
        if event["kind"] == "commit":
            set_assembling(event["slot"], event["manifest"])
        print(
            f"[gp_daemon] slot {event['slot']} recovered v6.3 {event['kind']} "
            f"id={event['request_id']}",
            flush=True,
        )
    for slot, pending in segmented_pending.items():
        set_assembling(slot, pending["manifest"])
        print(
            f"[gp_daemon] slot {slot} awaiting v6.2 parts id={pending['manifest']['id']}",
            flush=True,
        )
    for slot, job_path, job_text, transport in startup_jobs:
        worker = SlotWorker(slot, job_path, job_text, transport)
        workers[slot] = worker
        worker.start()
        print(f"[gp_daemon] slot {slot} recovered at startup", flush=True)

    while True:
        try:
            # Backup heartbeat path: even if the status thread ever exits, the
            # nonblocking scanner keeps liveness current during concurrent jobs.
            if time.monotonic() - _last_status_write > STATUS_EVERY * 2:
                write_status()
            if _source_signature() != source_signature:
                if not restart_pending:
                    print("[gp_daemon] source changed; restart pending", flush=True)
                restart_pending = True
            if restart_pending:
                if any(worker.is_alive() for worker in workers.values()):
                    time.sleep(POLL_INTERVAL)
                    continue
                print("[gp_daemon] source changed; restarting", flush=True)
                pidfile_handle.close()
                os.execv(sys.executable, [sys.executable, str(SOURCE_PATH)])

            now = time.monotonic()
            # Workers publish terminal response state before exiting. Retire
            # their immutable request files only after the thread is dead.
            for slot, event in list(v63_inflight.items()):
                worker = workers.get(slot)
                if worker is not None and worker.is_alive():
                    continue
                v63_inflight.pop(slot, None)
                manifest = event["manifest"]
                response_path = WORKSPACE / f"resp_s{slot}.json"
                if response_is_terminal_for_ack(
                    response_path,
                    manifest["sha12"],
                    request_id=manifest["id"],
                ):
                    retire_v63_event(event, v63_known, v63_seen, now=now)
                else:
                    # A terminal file write can fail independently of the
                    # upstream request. Replay it unless a newer generation
                    # already owns the lane; deterministic newest-wins ordering
                    # also bounds orphaned part files in this edge case.
                    v63_seen.pop(event["key"], None)
                    queued = v63_queue.get(slot)
                    if queued is None:
                        v63_queue[slot] = event
                    elif event["order"] > queued["order"]:
                        retire_v63_event(queued, v63_known, v63_seen, now=now)
                        v63_queue[slot] = event
                    else:
                        retire_v63_event(event, v63_known, v63_seen, now=now)

            prune_v63_metadata(v63_known, v63_seen, now=now)
            if now >= next_v63_scan:
                discoveries, v63_failures = scan_v63_commits(v63_known)
                candidates = [
                    event
                    for event in [*discoveries, *v63_failures]
                    if event["key"] not in v63_seen
                    and not (
                        event["slot"] in v63_inflight
                        and v63_inflight[event["slot"]]["key"] == event["key"]
                    )
                ]
                newest, losers = select_newest_v63_events(candidates)
                for loser in losers:
                    retire_v63_event(loser, v63_known, v63_seen, now=now)
                for slot, event in newest.items():
                    queued = v63_queue.get(slot)
                    if queued is not None and queued["order"] >= event["order"]:
                        retire_v63_event(event, v63_known, v63_seen, now=now)
                        continue
                    if queued is not None:
                        retire_v63_event(queued, v63_known, v63_seen, now=now)
                    v63_queue[slot] = event
                    print(
                        f"[gp_daemon] slot {slot} queued v6.3 {event['kind']} "
                        f"id={event['request_id']}",
                        flush=True,
                    )
                next_v63_scan = now + V63_SCAN_INTERVAL

            for slot, event in sorted(
                list(v63_queue.items()),
                key=lambda item: item[1]["order"],
            ):
                worker = workers.get(slot)
                if worker is not None and worker.is_alive():
                    continue
                try:
                    stat = event["commit_path"].stat()
                    current_signature = (stat.st_mtime_ns, stat.st_size)
                except OSError:
                    v63_queue.pop(slot, None)
                    clear_assembling(slot)
                    continue
                if current_signature != event["signature"]:
                    v63_queue.pop(slot, None)
                    clear_assembling(slot)
                    continue
                if event["kind"] == "failure":
                    if not publish_v63_failure_if_idle(
                        event,
                        worker,
                        v63_known,
                        v63_seen,
                        now=now,
                    ):
                        continue
                    v63_queue.pop(slot, None)
                    print(
                        f"[gp_daemon] slot {slot} published queued "
                        f"{event['code']} id={event['request_id']}",
                        flush=True,
                    )
                    continue
                manifest = event["manifest"]
                response_path = WORKSPACE / f"resp_s{slot}.json"
                if response_is_terminal_for_ack(
                    response_path,
                    manifest["sha12"],
                    request_id=manifest["id"],
                ):
                    v63_queue.pop(slot, None)
                    retire_v63_event(event, v63_known, v63_seen, now=now)
                    clear_assembling(slot)
                    continue
                try:
                    assembled = assemble_segmented_job(slot, manifest)
                except (SegmentAssemblyError, SegmentManifestError) as ex:
                    v63_queue.pop(slot, None)
                    write_slot_failed(
                        slot,
                        manifest["sha12"],
                        "bad_v63_parts",
                        str(ex),
                        transport=manifest,
                    )
                    retire_v63_event(event, v63_known, v63_seen, now=now)
                    continue
                if assembled is None:
                    set_assembling(slot, manifest)
                    continue
                v63_queue.pop(slot, None)
                clear_assembling(slot)
                worker = SlotWorker(
                    slot,
                    event["commit_path"],
                    assembled,
                    manifest,
                )
                workers[slot] = worker
                v63_inflight[slot] = event
                mark_v63_seen(v63_seen, event["key"], now=now)
                worker.start()
                print(
                    f"[gp_daemon] slot {slot} v6.3 assembled and dispatched "
                    f"id={manifest['id']}",
                    flush=True,
                )

            for slot, pending in list(segmented_pending.items()):
                worker = workers.get(slot)
                if worker is not None and worker.is_alive():
                    if worker.transport.get("id") == pending["manifest"]["id"]:
                        segmented_pending.pop(slot, None)
                    continue
                try:
                    stat = pending["job_path"].stat()
                    current_signature = (stat.st_mtime_ns, stat.st_size)
                except OSError:
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                    continue
                if current_signature != pending["signature"]:
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                    continue
                response_path = WORKSPACE / f"resp_s{slot}.json"
                if response_is_terminal_for_ack(
                    response_path,
                    pending["manifest"]["sha12"],
                    request_id=pending["manifest"]["id"],
                ):
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                    continue
                try:
                    assembled = assemble_segmented_job(slot, pending["manifest"])
                except SegmentAssemblyError as ex:
                    segmented_pending.pop(slot, None)
                    write_slot_failed(
                        slot,
                        pending["manifest"]["sha12"],
                        "bad_v6_parts",
                        str(ex),
                    )
                    continue
                if assembled is None:
                    continue
                segmented_pending.pop(slot, None)
                clear_assembling(slot)
                worker = SlotWorker(
                    slot,
                    pending["job_path"],
                    assembled,
                    pending["manifest"],
                )
                workers[slot] = worker
                worker.start()
                print(f"[gp_daemon] slot {slot} v6.2 assembled and dispatched", flush=True)

            v63_owned_slots = set(v63_queue) | set(v63_inflight)
            v63_owned_slots.update(
                slot
                for slot, worker in workers.items()
                if worker.is_alive() and worker.transport.get("v") == 6.3
            )
            for slot, p in scan_slots(known):
                if slot in v63_owned_slots:
                    # Baseline doc echoes or stale v6.2 cursor content while
                    # the immutable v6.3 generation owns this physical lane.
                    snapshot = read_job_snapshot(p)
                    if snapshot is not None:
                        known[slot] = snapshot[0]
                    continue
                w = workers.get(slot)
                if w is not None and w.is_alive():
                    # Client watchdogs rewrite the same envelope when an ACK is
                    # delayed. Consume that mtime-only generation while its
                    # matching worker is alive, but preserve a genuinely new
                    # request for dispatch after this worker releases the lane.
                    snapshot = read_job_snapshot(p)
                    if snapshot is not None:
                        sig, job_text = snapshot
                        try:
                            manifest = parse_segment_manifest(job_text)
                            incoming_ack = (
                                manifest["sha12"]
                                if manifest is not None
                                else sha12(job_text)
                            )
                        except SegmentManifestError:
                            incoming_ack = None
                        if incoming_ack == w.ack:
                            known[slot] = sig
                    continue
                snapshot = read_job_snapshot(p)
                if snapshot is None:
                    continue
                sig, job_text = snapshot
                if known.get(slot) == sig:
                    continue
                known[slot] = sig
                # Empty slot files are the idle state, not failed jobs. This
                # also lets clients clear a completed slot without incrementing
                # the daemon's failure counter.
                if not job_text.strip():
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                    continue
                try:
                    manifest = parse_segment_manifest(job_text)
                except SegmentManifestError as ex:
                    segmented_pending.pop(slot, None)
                    write_slot_failed(
                        slot,
                        manifest_ack_hint(job_text),
                        "bad_v6_manifest",
                        str(ex),
                    )
                    continue
                if manifest is not None:
                    response_path = WORKSPACE / f"resp_s{slot}.json"
                    if response_is_terminal_for_ack(
                        response_path,
                        manifest["sha12"],
                        request_id=manifest["id"],
                    ):
                        # The immutable request id makes this a watchdog replay,
                        # not a new invocation with coincidentally equal input.
                        segmented_pending.pop(slot, None)
                        clear_assembling(slot)
                        print(
                            f"[gp_daemon] slot {slot} ignored terminal v6.2 replay "
                            f"id={manifest['id']}",
                            flush=True,
                        )
                        continue
                    try:
                        assembled = assemble_segmented_job(slot, manifest)
                    except SegmentAssemblyError as ex:
                        segmented_pending.pop(slot, None)
                        write_slot_failed(
                            slot,
                            manifest["sha12"],
                            "bad_v6_parts",
                            str(ex),
                        )
                        continue
                    if assembled is None:
                        segmented_pending[slot] = {
                            "signature": sig,
                            "job_path": p,
                            "manifest": manifest,
                        }
                        set_assembling(slot, manifest)
                        print(
                            f"[gp_daemon] slot {slot} awaiting v6.2 parts id={manifest['id']}",
                            flush=True,
                        )
                        continue
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                    job_text = assembled
                else:
                    segmented_pending.pop(slot, None)
                    clear_assembling(slot)
                t = SlotWorker(slot, p, job_text, manifest)
                workers[slot] = t
                t.start()
                print(f"[gp_daemon] slot {slot} dispatched", flush=True)
        except Exception:
            traceback.print_exc()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
