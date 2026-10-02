/**
 * In-process completion signals for reclamation ledger entries (phase-split piece iv, part 1).
 *
 * The phase split hands a caller a durable ledger entry instead of a synchronous result, but
 * both phased call sites still need the outcome. The historical-generation site reports a
 * mismatch upward and publishes the removed archives; the eviction loop must know whether space
 * was actually freed before it re-measures. The ledger row cannot carry that outcome --
 * archivedTranscripts is deliberately not durable -- so the values travel in-process, from
 * whoever drove the entry to terminal to whoever is waiting on it.
 *
 * Claude's 2026-10-01 call-site-wiring ruling, with Vern's completion-notification amendment,
 * fixes the shape:
 *
 *  - the signal carries the consumer values, mapped from the terminal ledger state, so a waiting
 *    call site keeps its existing branch logic unchanged;
 *  - a DECLINE resolves the signal, it does not reject it -- declines are normal outcomes, and
 *    this is the third place the ruling has had to say so;
 *  - a waiter that arrives after the entry is already terminal resolves from the ledger row
 *    instead of waiting for a notification that can never arrive -- the registered-too-late bug,
 *    the same family as the lost wakeup and the one way this design can hang a caller forever;
 *  - the signal is two-tier: in-process only. Across a process death there is nobody to notify,
 *    which is already covered -- the row is durable, a later pull finishes the work, and
 *    publication has its own repair path.
 *
 * One signal per ledger entry, resolvable independently, with no assumption that exactly one is
 * outstanding: the eviction loop is expected to enqueue several candidates and react to each as
 * it completes once that loop is refactored (deliberately not part of the wiring).
 */
import type { DatabaseSync } from "node:sqlite";
import {
  readReclamationLedgerEntry,
  type ReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import type { SessionLifecycleArchivedTranscript } from "./session-accessor.sqlite-contract.js";

/** The values the phased call sites consumed from the atomic result, and nothing more. */
export type ReclamationLedgerCompletion = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
  deleted: boolean;
  expectedEntryMismatch?: true;
};

type CompletionResolver = {
  reject: (error: unknown) => void;
  resolve: (completion: ReclamationLedgerCompletion) => void;
};

/**
 * Handles are keyed by the live database handle rather than the entry id alone: ids are
 * per-database autoincrements, so two agent databases in one gateway would otherwise collide on
 * their first entry. A WeakMap keeps signals attached to the handle the accessor caches and lets
 * a closed database's map be collected.
 */
const completionResolvers = new WeakMap<DatabaseSync, Map<number, CompletionResolver[]>>();

function readResolverMap(database: DatabaseSync): Map<number, CompletionResolver[]> | undefined {
  return completionResolvers.get(database);
}

function writeResolverMap(database: DatabaseSync): Map<number, CompletionResolver[]> {
  let map = completionResolvers.get(database);
  if (!map) {
    map = new Map<number, CompletionResolver[]>();
    completionResolvers.set(database, map);
  }
  return map;
}

function takeResolvers(database: DatabaseSync, id: number): CompletionResolver[] {
  const map = readResolverMap(database);
  const resolvers = map?.get(id) ?? [];
  map?.delete(id);
  return resolvers;
}

function reclamationCompletionFailure(entry: ReclamationLedgerEntry): Error {
  return new Error(
    "Reclamation ledger entry " + entry.id + " failed: " + (entry.lastError ?? "no recorded error"),
  );
}

/**
 * Maps an already-terminal row to the consumer values. archivedTranscripts is empty on this path
 * by construction: the row does not carry it, and an archive a crashed process left unpublished
 * is repaired by the pending-archive sweep, not here.
 */
function terminalCompletion(
  entry: ReclamationLedgerEntry,
): ReclamationLedgerCompletion | undefined {
  if (entry.state === "done") {
    return { archivedTranscripts: [], deleted: entry.sessionFullyDeleted };
  }
  if (entry.state === "declined") {
    return { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true };
  }
  return undefined;
}

/** Delivers the values the driver observed to every waiter on the entry, then forgets them. */
export function settleReclamationLedgerCompletion(
  database: DatabaseSync,
  id: number,
  completion: ReclamationLedgerCompletion,
): void {
  for (const resolver of takeResolvers(database, id)) {
    resolver.resolve(completion);
  }
}

/** Rejects every waiter on a terminal failed entry; a failure is not a value. */
export function failReclamationLedgerCompletion(
  database: DatabaseSync,
  id: number,
  error: unknown,
): void {
  for (const resolver of takeResolvers(database, id)) {
    resolver.reject(error);
  }
}

export function hasReclamationLedgerCompletionWatchers(
  database: DatabaseSync,
  id: number,
): boolean {
  return (readResolverMap(database)?.get(id)?.length ?? 0) > 0;
}

/**
 * Resolves with the entry outcome. The ledger read and the registration are both synchronous, so
 * no settle can interleave between them: a waiter either finds the terminal row here or is
 * registered before any later settle runs.
 */
export function awaitReclamationLedgerCompletion(
  database: DatabaseSync,
  id: number,
): Promise<ReclamationLedgerCompletion> {
  const entry = readReclamationLedgerEntry(database, id);
  if (!entry) {
    return Promise.reject(new Error("Reclamation ledger entry " + id + " is missing"));
  }
  const terminal = terminalCompletion(entry);
  if (terminal) {
    return Promise.resolve(terminal);
  }
  if (entry.state === "failed") {
    return Promise.reject(reclamationCompletionFailure(entry));
  }
  return new Promise<ReclamationLedgerCompletion>((resolve, reject) => {
    const map = writeResolverMap(database);
    const resolvers = map.get(id) ?? [];
    resolvers.push({ reject, resolve });
    map.set(id, resolvers);
  });
}
