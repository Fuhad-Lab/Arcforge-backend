/**
 * Forgvi Engine — nuclear-family agent messaging (Vube spec pillar 3).
 *
 * "Restrict parent/child cross-agent communications to direct message
 * passing (agent_message.send(...)) and save all step histories in
 * append-only JSONL files."
 *
 * Each run owns ONE mailbox. The chief, its specialist subagents, and the
 * verifier may exchange direct messages; anything addressed outside the
 * family is rejected (no lateral cross-run chatter, no fan-out to agents
 * that are not part of this run). Every message is journaled (which also
 * appends it to the run's JSONL step history) and kept in an in-memory
 * inbox the parent replays to spawned subagents.
 */

import { randomUUID } from "node:crypto";

/** The family roles a message can legitimately address. */
export const FAMILY_ROLES = new Set(["chief", "verifier", "user", "specialist"]);

export class AgentMailbox {
  /** @param {import('./journal.js').Journal} journal */
  constructor(journal, { family = ["chief", "verifier", "user"] } = {}) {
    this.journal = journal;
    this.runId = journal.runId;
    /** Roles that exist in this run from the start; specialists register on spawn. */
    this.family = new Set(family);
    /** @type {Array<any>} full message log (also in the JSONL via journal). */
    this.log = [];
  }

  /** A specialist joins the family when spawned (by persona key). */
  registerMember(member) {
    const key = String(member ?? "").trim();
    if (key) this.family.add(key);
  }

  /**
   * Direct message passing. Returns the delivered record, or an honest
   * rejection object when addressing outside the nuclear family.
   */
  send({ from, to, text, kind = "note", payload } = {}) {
    const sender = String(from ?? "unknown").trim();
    const recipient = String(to ?? "").trim();
    const body = String(text ?? "").trim();
    if (!recipient) return { ok: false, error: "agent_message.send: `to` is required" };
    if (!body && payload == null) return { ok: false, error: "agent_message.send: empty message" };
    const isFamily =
      this.family.has(recipient) ||
      // specialist messages use the persona key as the address
      [...this.family].some((m) => recipient === `${m}`) ||
      FAMILY_ROLES.has(recipient);
    if (!isFamily) {
      return {
        ok: false,
        error: `agent_message.send: "${recipient}" is outside this run's nuclear family (${[...this.family].join(", ")})`,
      };
    }
    const record = {
      messageId: randomUUID(),
      from: sender,
      to: recipient,
      kind,
      text: body,
      payload: payload ?? undefined,
      ts: Date.now(),
    };
    this.log.push(record);
    this.journal.emit(
      {
        type: "agent_message",
        from: record.from,
        to: record.to,
        kind: record.kind,
        text: record.text.slice(0, 2000),
      },
      { role: record.from },
    );
    return { ok: true, message: record };
  }

  /** Messages addressed to one member (parent replays these to subagents). */
  inboxFor(member) {
    return this.log.filter((m) => m.to === member);
  }

  /** Full history (already persisted in the run's JSONL via the journal). */
  history() {
    return [...this.log];
  }
}
