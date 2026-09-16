export type OwnedTextChunk<T> = {
  owner: T;
  text: string;
};

type PendingChunk<T> = OwnedTextChunk<T> & {
  points: number;
};

type StopSequence = {
  source: string;
  points: number[];
  failure: number[];
  matched: number;
  order: number;
};

type StopMatch = {
  sequence: StopSequence;
  start: number;
};

export type UnicodeStopFilterResult<T> = {
  chunks: OwnedTextChunk<T>[];
  matchedSequence: string | null;
};

function isHighSurrogate(code: number): boolean {
  return code >= 0xd800 && code <= 0xdbff;
}

function isLowSurrogate(code: number): boolean {
  return code >= 0xdc00 && code <= 0xdfff;
}

function codePoints(value: string): number[] {
  const points: number[] = [];
  for (let index = 0; index < value.length; index += 1) {
    const point = value.codePointAt(index)!;
    points.push(point);
    if (point > 0xffff) index += 1;
  }
  return points;
}

function appendChunk<T>(chunks: OwnedTextChunk<T>[], owner: T, text: string): void {
  if (!text) return;
  const last = chunks.at(-1);
  if (last?.owner === owner) last.text += text;
  else chunks.push({ owner, text });
}

function detachedCopy(value: string): string {
  return value.length === 0 ? "" : Buffer.from(value, "utf8").toString("utf8");
}

function codeUnitOffsetAtPoint(value: string, prefixPoints: number, totalPoints: number): number {
  if (prefixPoints <= 0) return 0;
  if (prefixPoints >= totalPoints) return value.length;
  if (prefixPoints <= totalPoints - prefixPoints) {
    let index = 0;
    for (let seen = 0; seen < prefixPoints; seen += 1) {
      const code = value.charCodeAt(index);
      index += isHighSurrogate(code) ? 2 : 1;
    }
    return index;
  }

  let index = value.length;
  for (let remaining = totalPoints - prefixPoints; remaining > 0; remaining -= 1) {
    index -= 1;
    if (isLowSurrogate(value.charCodeAt(index))) index -= 1;
  }
  return index;
}

function validatedSpan(
  value: string,
  start: number,
): { end: number; points: number; danglingHigh: number | null } {
  let points = 0;
  let index = start;
  while (index < value.length) {
    const code = value.charCodeAt(index);
    if (isHighSurrogate(code)) {
      if (index + 1 >= value.length) {
        return { end: index, points, danglingHigh: code };
      }
      if (!isLowSurrogate(value.charCodeAt(index + 1))) {
        throw new TypeError("Responses stream contained an invalid astral character");
      }
      index += 2;
    } else {
      if (isLowSurrogate(code)) {
        throw new TypeError("Responses stream contained an invalid astral character");
      }
      index += 1;
    }
    points += 1;
  }
  return { end: value.length, points, danglingHigh: null };
}

export function assertUnicodeWellFormed(value: string): void {
  try {
    const span = validatedSpan(value, 0);
    if (span.danglingHigh === null) return;
  } catch {
    // Public data validation has one stable error regardless of surrogate shape.
  }
  throw new TypeError("Responses data contains an invalid Unicode scalar value");
}

/** Retains only a bounded Unicode-code-point suffix needed for stop matching. */
export class UnicodeStopFilter<T> {
  readonly #sequences: StopSequence[];
  readonly #maxSuffixPoints: number;
  #pending: Array<PendingChunk<T> | null> = [];
  #pendingHead = 0;
  #pendingPoints = 0;
  #processedPoints = 0;
  #danglingHigh: { owner: T; code: number } | null = null;
  #pendingMatch: StopMatch | null = null;
  #matchedSequence: string | null = null;

  constructor(stopSequences: readonly string[]) {
    this.#sequences = stopSequences.map((source, order) => {
      const points = codePoints(source);
      const failure = new Array<number>(points.length).fill(0);
      for (let index = 1, prefix = 0; index < points.length; index += 1) {
        while (prefix > 0 && points[index] !== points[prefix]) prefix = failure[prefix - 1];
        if (points[index] === points[prefix]) prefix += 1;
        failure[index] = prefix;
      }
      return { source, points, failure, matched: 0, order };
    });
    this.#maxSuffixPoints = Math.max(
      0,
      ...this.#sequences.map(sequence => sequence.points.length - 1),
    );
  }

  append(owner: T, value: string): UnicodeStopFilterResult<T> {
    if (this.#matchedSequence !== null || value.length === 0) {
      return { chunks: [], matchedSequence: this.#matchedSequence };
    }
    if (this.#sequences.length === 0) return this.#appendWithoutStops(owner, value);

    const pendingStart = this.#processedPoints - this.#pendingPoints;
    const match = this.#appendForMatching(owner, value);
    if (match !== null) {
      const chunks = this.#take(match.start - pendingStart);
      this.#discardPending();
      this.#danglingHigh = null;
      this.#matchedSequence = match.sequence.source;
      this.#resetMatchers();
      return { chunks, matchedSequence: this.#matchedSequence };
    }
    return {
      chunks: this.#take(Math.max(0, this.#pendingPoints - this.#maxSuffixPoints)),
      matchedSequence: null,
    };
  }

  endSegment(): UnicodeStopFilterResult<T> {
    if (this.#matchedSequence !== null) {
      this.#discardPending();
      this.#danglingHigh = null;
      this.#resetMatchers();
      return { chunks: [], matchedSequence: this.#matchedSequence };
    }
    if (this.#danglingHigh !== null) {
      throw new TypeError("Responses stream ended ordinary text within an astral character");
    }
    if (this.#pendingMatch !== null) {
      // A stop cannot continue across a non-text boundary. Any completed
      // candidate therefore wins over the unfinished prefixes held beside it.
      const start = this.#processedPoints - this.#pendingPoints;
      const chunks = this.#take(this.#pendingMatch.start - start);
      this.#matchedSequence = this.#pendingMatch.sequence.source;
      this.#discardPending();
      this.#resetMatchers();
      return { chunks, matchedSequence: this.#matchedSequence };
    }
    const chunks = this.#take(this.#pendingPoints);
    this.#resetMatchers();
    return { chunks, matchedSequence: null };
  }

  hasPending(owner: T): boolean {
    if (this.#danglingHigh?.owner === owner) return true;
    for (let index = this.#pendingHead; index < this.#pending.length; index += 1) {
      if (this.#pending[index]?.owner === owner) return true;
    }
    return false;
  }

  retainedCodePoints(): number {
    return this.#pendingPoints + (this.#danglingHigh === null ? 0 : 1);
  }

  clear(): void {
    this.#discardPending();
    this.#danglingHigh = null;
    this.#matchedSequence = null;
    this.#resetMatchers();
  }

  #appendWithoutStops(owner: T, value: string): UnicodeStopFilterResult<T> {
    const chunks: OwnedTextChunk<T>[] = [];
    const priorHigh = this.#danglingHigh;
    let start = 0;
    if (priorHigh !== null) {
      const low = value.charCodeAt(0);
      if (!isLowSurrogate(low)) {
        throw new TypeError("Responses stream split an invalid astral character");
      }
      start = 1;
    }
    const span = validatedSpan(value, start);
    this.#danglingHigh = null;
    if (priorHigh !== null) {
      appendChunk(chunks, priorHigh.owner, String.fromCharCode(priorHigh.code, value.charCodeAt(0)));
    }
    if (span.end > start) appendChunk(chunks, owner, value.slice(start, span.end));
    if (span.danglingHigh !== null) {
      this.#danglingHigh = { owner, code: span.danglingHigh };
    }
    return { chunks, matchedSequence: null };
  }

  #appendForMatching(owner: T, value: string): StopMatch | null {
    const priorHigh = this.#danglingHigh;
    let start = 0;
    if (priorHigh !== null) {
      const low = value.charCodeAt(0);
      if (!isLowSurrogate(low)) {
        throw new TypeError("Responses stream split an invalid astral character");
      }
      start = 1;
    }
    const span = validatedSpan(value, start);
    let selected = this.#pendingMatch;
    const observe = (point: number): void => {
      const candidate = this.#advanceMatchers(point);
      if (candidate !== null && (
        selected === null || candidate.start < selected.start ||
        (candidate.start === selected.start &&
          candidate.sequence.order < selected.sequence.order)
      )) selected = candidate;
    };

    this.#danglingHigh = null;
    if (priorHigh !== null) {
      const low = value.charCodeAt(0);
      observe(((priorHigh.code - 0xd800) << 10) + (low - 0xdc00) + 0x10000);
      this.#enqueue(
        priorHigh.owner,
        String.fromCharCode(priorHigh.code, low),
        1,
      );
    }

    for (let index = start; index < span.end;) {
      const point = value.codePointAt(index)!;
      observe(point);
      index += point > 0xffff ? 2 : 1;
    }
    if (span.end > start) {
      this.#enqueue(owner, value.slice(start, span.end), span.points);
    }
    if (span.danglingHigh !== null) {
      this.#danglingHigh = { owner, code: span.danglingHigh };
    }
    this.#pendingMatch = selected;
    if (selected !== null) {
      for (const sequence of this.#sequences) {
        if (sequence.matched === 0) continue;
        const start = this.#processedPoints - sequence.matched;
        // KMP retains the longest matching prefix, hence its earliest possible
        // start. Shorter suffixes cannot beat a candidate this prefix cannot.
        // While a contender exists, the candidate remains within the same
        // maximum-length-minus-one suffix already retained between appends.
        if (start < selected.start || (start === selected.start &&
          sequence.order < selected.sequence.order)) return null;
      }
    }
    return selected;
  }

  #advanceMatchers(point: number): StopMatch | null {
    const position = this.#processedPoints;
    let selected: StopMatch | null = null;
    for (const sequence of this.#sequences) {
      if (sequence.points.length === 0) continue;
      let matched = sequence.matched;
      while (matched > 0 && point !== sequence.points[matched]) {
        matched = sequence.failure[matched - 1];
      }
      if (point === sequence.points[matched]) matched += 1;
      if (matched === sequence.points.length) {
        const candidate = { sequence, start: position - sequence.points.length + 1 };
        if (selected === null || candidate.start < selected.start ||
          (candidate.start === selected.start && sequence.order < selected.sequence.order)) {
          selected = candidate;
        }
        matched = sequence.failure[matched - 1] ?? 0;
      }
      sequence.matched = matched;
    }
    this.#processedPoints += 1;
    return selected;
  }

  #enqueue(owner: T, text: string, points: number): void {
    if (points <= 0 || text.length === 0) return;
    this.#pending.push({ owner, text, points });
    this.#pendingPoints += points;
  }

  #take(count: number): OwnedTextChunk<T>[] {
    if (!Number.isSafeInteger(count) || count < 0 || count > this.#pendingPoints) {
      throw new RangeError("Unicode stop filter pending range is invalid");
    }
    const chunks: OwnedTextChunk<T>[] = [];
    let remaining = count;
    while (remaining > 0) {
      const entry = this.#pending[this.#pendingHead];
      if (!entry) throw new RangeError("Unicode stop filter pending state is invalid");
      if (entry.points <= remaining) {
        appendChunk(chunks, entry.owner, entry.text);
        remaining -= entry.points;
        this.#pendingPoints -= entry.points;
        this.#pending[this.#pendingHead] = null;
        this.#pendingHead += 1;
        continue;
      }

      const offset = codeUnitOffsetAtPoint(entry.text, remaining, entry.points);
      appendChunk(chunks, entry.owner, entry.text.slice(0, offset));
      entry.text = detachedCopy(entry.text.slice(offset));
      entry.points -= remaining;
      this.#pendingPoints -= remaining;
      remaining = 0;
    }
    this.#compactPending();
    return chunks;
  }

  #compactPending(): void {
    if (this.#pendingHead === this.#pending.length) {
      this.#pending = [];
      this.#pendingHead = 0;
    } else if (this.#pendingHead >= 64 && this.#pendingHead * 2 >= this.#pending.length) {
      this.#pending = this.#pending.slice(this.#pendingHead);
      this.#pendingHead = 0;
    }
  }

  #discardPending(): void {
    this.#pending = [];
    this.#pendingHead = 0;
    this.#pendingPoints = 0;
  }

  #resetMatchers(): void {
    this.#processedPoints = 0;
    this.#pendingMatch = null;
    for (const sequence of this.#sequences) sequence.matched = 0;
  }
}
