export type OwnedTextChunk<T> = {
  owner: T;
  text: string;
};

type OwnedCodePoint<T> = {
  owner: T;
  value: string;
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

/** Retains only a bounded Unicode-code-point suffix needed for stop matching. */
export class UnicodeStopFilter<T> {
  readonly #sequences: Array<{ source: string; points: string[]; failure: number[] }>;
  readonly #maxSuffixPoints: number;
  #points: OwnedCodePoint<T>[] = [];
  #danglingHigh: OwnedCodePoint<T> | null = null;
  #matchedSequence: string | null = null;

  constructor(stopSequences: readonly string[]) {
    this.#sequences = stopSequences.map(source => {
      const points = Array.from(source);
      const failure = new Array<number>(points.length).fill(0);
      for (let index = 1, prefix = 0; index < points.length; index += 1) {
        while (prefix > 0 && points[index] !== points[prefix]) prefix = failure[prefix - 1];
        if (points[index] === points[prefix]) prefix += 1;
        failure[index] = prefix;
      }
      return { source, points, failure };
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
    this.#appendCodePoints(owner, value);
    const match = this.#findMatch();
    if (match !== null) {
      const chunks = this.#take(match.offset);
      this.#points = [];
      this.#danglingHigh = null;
      this.#matchedSequence = match.sequence;
      return { chunks, matchedSequence: match.sequence };
    }
    return {
      chunks: this.#take(Math.max(0, this.#points.length - this.#maxSuffixPoints)),
      matchedSequence: null,
    };
  }

  endSegment(): OwnedTextChunk<T>[] {
    if (this.#matchedSequence !== null) {
      this.#points = [];
      this.#danglingHigh = null;
      return [];
    }
    if (this.#danglingHigh !== null) {
      throw new TypeError("Responses stream ended ordinary text within an astral character");
    }
    return this.#take(this.#points.length);
  }

  hasPending(owner: T): boolean {
    return this.#danglingHigh?.owner === owner || this.#points.some(point => point.owner === owner);
  }

  retainedCodePoints(): number {
    return this.#points.length + (this.#danglingHigh === null ? 0 : 1);
  }

  clear(): void {
    this.#points = [];
    this.#danglingHigh = null;
    this.#matchedSequence = null;
  }

  #appendCodePoints(owner: T, value: string): void {
    let index = 0;
    if (this.#danglingHigh !== null) {
      const first = value.charCodeAt(0);
      if (value.length > 0 && isLowSurrogate(first)) {
        this.#points.push({
          owner: this.#danglingHigh.owner,
          value: this.#danglingHigh.value + value[0],
        });
        index = 1;
      } else {
        throw new TypeError("Responses stream split an invalid astral character");
      }
      this.#danglingHigh = null;
    }

    while (index < value.length) {
      const code = value.charCodeAt(index);
      if (isHighSurrogate(code)) {
        if (index + 1 >= value.length) {
          this.#danglingHigh = { owner, value: value[index] };
          return;
        }
        const low = value.charCodeAt(index + 1);
        if (!isLowSurrogate(low)) {
          throw new TypeError("Responses stream contained an invalid astral character");
        }
        this.#points.push({ owner, value: value.slice(index, index + 2) });
        index += 2;
      } else {
        if (isLowSurrogate(code)) {
          throw new TypeError("Responses stream contained an invalid astral character");
        }
        this.#points.push({ owner, value: value[index] });
        index += 1;
      }
    }
  }

  #findMatch(): { sequence: string; offset: number } | null {
    let selected: { sequence: string; offset: number } | null = null;
    for (const sequence of this.#sequences) {
      if (sequence.points.length === 0 || sequence.points.length > this.#points.length) continue;
      let matchedPoints = 0;
      for (let index = 0; index < this.#points.length; index += 1) {
        while (matchedPoints > 0 &&
          this.#points[index].value !== sequence.points[matchedPoints]) {
          matchedPoints = sequence.failure[matchedPoints - 1];
        }
        if (this.#points[index].value === sequence.points[matchedPoints]) matchedPoints += 1;
        if (matchedPoints === sequence.points.length) {
          const offset = index - sequence.points.length + 1;
          if (selected === null || offset < selected.offset) {
            selected = { sequence: sequence.source, offset };
          }
          break;
        }
      }
    }
    return selected;
  }

  #take(count: number): OwnedTextChunk<T>[] {
    if (count <= 0) return [];
    const points = this.#points.splice(0, count);
    const chunks: OwnedTextChunk<T>[] = [];
    for (const point of points) {
      const last = chunks.at(-1);
      if (last?.owner === point.owner) last.text += point.value;
      else chunks.push({ owner: point.owner, text: point.value });
    }
    return chunks;
  }
}
