/**
 * Forgvi Engine — the event journal.
 *
 * Every run journals its events (the engine never loses history), and
 * every SSE subscriber gets: full replay from seq 0 (or ?since=N), then
 * live events, then the terminal envelope and a clean close.
 *
 * Envelope shape — the contract the Forge frontend consumes:
 * {
 *   seq, ts, id, runId, sessionId, goalId?, iteration?, role?,
 *   event: { type: string } & Record<string, unknown>
 * }
 *
 * PERSISTENCE (the in-VM reload-survival contract): when ENGINE_PERSIST_DIR
 * is set, every emitted envelope is ALSO appended as one NDJSON line to
 * <persistDir>/<runId>.ndjson. On the VM's disk (not tmpfs) this survives
 * PM2 restarts and VM stop/start, so RunManager boot-recovery can rebuild
 * any run — finished runs replay identically, interrupted runs get an
 * honest terminal "incomplete" event appended. The host (Render) engine
 * leaves it unset and behaves exactly as before (RAM-only).
 */

import { appendFileSync, mkdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { randomUUID } from "node:crypto";

/** Marker appended when a recovered run is finalized (see recoverJournal). */
export const RECOVERED_CLOSE_EVENT = "forge-journal-recovered";

export class Journal {
  /**
   * @param {string} runId
   * @param {string} sessionId
   * @param {string} goalId
   * @param {{persistDir?: string, entries?: any[], seq?: number, closed?: boolean}} [opts]
   */
  constructor(runId, sessionId, goalId, opts = {}) {
    this.runId = runId;
    this.sessionId = sessionId;
    this.goalId = goalId;
    this.seq = opts.seq ?? 0;
    /** @type {Array<any>} */
    this.entries = opts.entries ?? [];
    /** @type {Set<any>} SSE writer set */
    this.subscribers = new Set();
    this.closed = opts.closed ?? false;
    this.persistPath = opts.persistDir
      ? join(resolve(opts.persistDir), `${runId}.ndjson`)
      : null;
    if (this.persistPath) {
      try {
        mkdirSync(resolve(opts.persistDir), { recursive: true });
      } catch {
        /* best-effort — persistence must never kill a run */
      }
    }
  }

  /**
   * Append an event to the journal and broadcast to live subscribers.
   * Returns the envelope.
   */
  emit(event, meta = {}) {
    if (this.closed) return null;
    const envelope = {
      seq: ++this.seq,
      ts: Date.now(),
      id: randomUUID(),
      runId: this.runId,
      sessionId: this.sessionId,
      goalId: this.goalId,
      iteration: meta.iteration ?? undefined,
      role: meta.role ?? undefined,
      event,
    };
    this.entries.push(envelope);
    this.#persist(envelope);
    const frame = `data: ${JSON.stringify(envelope)}\n\n`;
    for (const sub of this.subscribers) sub(frame);
    return envelope;
  }

  /** Synchronous NDJSON append — persistence must never lose an event to
   * a crash race (async appends can be in flight when the process dies),
   * and the volume is low (a few short lines per second at peak). */
  #persist(envelope) {
    if (!this.persistPath) return;
    try {
      appendFileSync(this.persistPath, `${JSON.stringify(envelope)}\n`, "utf8");
    } catch {
      /* best-effort by design */
    }
  }

  /**
   * Attach an SSE writer. Sends replay (from `since`), then live frames.
   * Returns a detach function.
   */
  attach(writer, since = 0) {
    for (const envelope of this.entries) {
      if (envelope.seq > since) writer(`data: ${JSON.stringify(envelope)}\n\n`);
    }
    if (this.closed) {
      writer("event: forge-close\ndata: closed\n\n");
      return () => {};
    }
    this.subscribers.add(writer);
    return () => this.subscribers.delete(writer);
  }

  /** Terminal: mark closed, drop subscribers (they each got the replay). */
  close() {
    this.closed = true;
    for (const sub of this.subscribers) sub("event: forge-close\ndata: closed\n\n");
    this.subscribers.clear();
  }
}

/**
 * Recover a persisted journal from <persistDir>/<runId>.ndjson.
 *
 * Returns null when the file is absent/unreadable. Rebuilds entries + seq;
 * `closed` is derived from the event stream itself: a run whose last
 * journaled event is `run_finished` is terminal (its `close()` happened);
 * anything else was interrupted mid-flight (crash / restart / VM stop) and
 * the caller finalizes it honestly with `finalizeRecovered()`.
 *
 * @returns {{ journal: Journal, finished: boolean, lastFinish: any | null } | null}
 */
export function recoverJournal(runId, sessionId, goalId, persistDir) {
  const path = join(resolve(persistDir), `${runId}.ndjson`);
  let raw;
  try {
    raw = readFileSync(path, "utf8");
  } catch {
    return null;
  }
  const entries = [];
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      entries.push(JSON.parse(trimmed));
    } catch {
      /* a torn final line (crash mid-append) — skip it */
    }
  }
  if (entries.length === 0) return null;
  const seq = entries[entries.length - 1].seq ?? entries.length;
  let lastFinish = null;
  for (let i = entries.length - 1; i >= 0; i--) {
    if (entries[i].event?.type === "run_finished") {
      lastFinish = entries[i];
      break;
    }
  }
  const journal = new Journal(runId, sessionId, goalId, {
    persistDir,
    entries,
    seq,
    closed: lastFinish !== null,
  });
  return { journal, finished: lastFinish !== null, lastFinish };
}
