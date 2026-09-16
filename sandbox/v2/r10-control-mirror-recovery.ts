const LAG_GRACE_MS = 5_000;
const MIRROR_FRESH_MS = 10_000;
const RECONNECT_COOLDOWN_MS = 10_000;
const MAX_RECONNECTS_WITHOUT_NATIVE_PROGRESS = 3;

/**
 * A fixed-size reconnect hint, never an execution or acknowledgement authority.
 * Callers provide validated native/mirrored CONTROL identities and local time.
 * Revision high-waters survive disconnect, stale resync, and hint expiry.
 */
export class R10ControlMirrorRecovery {
  #clientId: number | null = null;
  #hostInstanceId: string | null = null;
  #nativeRevision = -1;
  #mirrorRevision = -1;
  #mirrorAtMs: number | null = null;
  #lagSinceMs: number | null = null;
  #attemptedMirrorRevision = -1;
  #lastAttemptAtMs: number | null = null;
  #attempts = 0;

  get clientId(): number | null { return this.#clientId; }
  get hostInstanceId(): string | null { return this.#hostInstanceId; }

  native(clientId: number, hostInstanceId: string, revision: number, now: number): void {
    if (
      (this.#hostInstanceId !== null && hostInstanceId !== this.#hostInstanceId) ||
      revision <= this.#nativeRevision
    ) return;
    if (clientId !== this.#clientId) {
      this.#clientId = clientId;
      this.#hostInstanceId = hostInstanceId;
      this.invalidateMirror();
    }
    this.#nativeRevision = revision;
    this.#attempts = 0;
    // Native progress restarts the grace even if its revision still trails a
    // newer mirror. Healthy propagation skew must not churn the connection.
    this.#lagSinceMs = this.#mirrorRevision > revision && this.#fresh(now)
      ? now : null;
  }

  mirror(revision: number, now: number): void {
    if (revision < this.#mirrorRevision || revision <= this.#nativeRevision) {
      this.invalidateMirror();
      return;
    }
    if (revision === this.#mirrorRevision) return;
    if (!this.#fresh(now) || this.#lagSinceMs === null) this.#lagSinceMs = now;
    this.#mirrorRevision = revision;
    this.#mirrorAtMs = now;
  }

  invalidateMirror(): void {
    this.#mirrorAtMs = null;
    this.#lagSinceMs = null;
  }

  nextDelayMs(now: number): number | null {
    if (
      this.#clientId === null ||
      this.#attempts >= MAX_RECONNECTS_WITHOUT_NATIVE_PROGRESS ||
      this.#mirrorRevision <= this.#nativeRevision ||
      this.#mirrorRevision <= this.#attemptedMirrorRevision ||
      this.#lagSinceMs === null ||
      !this.#fresh(now)
    ) return null;
    const due = Math.max(
      this.#lagSinceMs + LAG_GRACE_MS,
      (this.#lastAttemptAtMs ?? -Infinity) + RECONNECT_COOLDOWN_MS,
    );
    // No timer may outlive the evidence that justified it. A genuinely newer
    // mirror may later refresh freshness, but rereading an old one cannot.
    if (due > this.#mirrorAtMs! + MIRROR_FRESH_MS) return null;
    return Math.max(0, due - now);
  }

  beginAttempt(now: number): boolean {
    if (this.nextDelayMs(now) !== 0) return false;
    this.#attemptedMirrorRevision = this.#mirrorRevision;
    this.#lastAttemptAtMs = now;
    this.#attempts++;
    return true;
  }

  #fresh(now: number): boolean {
    return this.#mirrorAtMs !== null && now - this.#mirrorAtMs <= MIRROR_FRESH_MS;
  }
}
