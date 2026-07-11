import { foldRecords } from './record.js'
import type { FoldedRecord, RecordFormat, RecordSibling } from './record.js'

/**
 * A live editing session over a record value — the doc-handle the spec's §4.3
 * and §6.15 require, and the edit/absorb half of the read-fold-edit-write
 * discipline.
 *
 * Divergence enters a replica through sync at arbitrary times, including while
 * a document is open for editing. An application that keeps committing from its
 * pre-arrival in-memory document supersedes the incoming sibling with a value
 * that does not contain those changes — a lost edit. The fix is the standard
 * automerge pattern: {@link RecordSession.absorb} the arriving sibling *into*
 * the live document, so pending edits survive and the next commit's `Replace`
 * is inclusive rather than data-losing.
 *
 * The session is deliberately transport-agnostic: it holds and merges the live
 * value; the application wires it to its own sync/subscription plumbing (feed
 * arrivals to {@link RecordSession.absorb}, take {@link RecordSession.encode}'s
 * bytes to commit). All work here is synchronous CPU and is meant to run off
 * the UI thread, beside the transactor — the main thread is handed one merged
 * value, never a set of siblings.
 */
export class RecordSession<T> {
  #format: RecordFormat<T>
  #value: T

  constructor(format: RecordFormat<T>, initial: T) {
    this.#format = format
    this.#value = initial
  }

  /**
   * Open a session over a `(the, of)` group's siblings, folding them first
   * (progressive open, §4.3): the caller sees one merged value immediately, and
   * the fold is inclusive of every stored sibling. Returns `undefined` for an
   * empty group, and — when every sibling was undecodable — reports it via the
   * returned {@link FoldedRecord} rather than opening on a value that isn't
   * there.
   */
  static open<T>(
    format: RecordFormat<T>,
    rows: readonly RecordSibling[]
  ): { session?: RecordSession<T>; folded: FoldedRecord<T> } | undefined {
    const folded = foldRecords(rows, format)
    if (!folded) return undefined
    if (folded.value === undefined) return { folded }
    return { session: new RecordSession(format, folded.value), folded }
  }

  /** The live value. */
  get value(): T {
    return this.#value
  }

  /**
   * Apply a local edit. `edit` receives the current value and returns the
   * edited one (automerge documents are immutable, so this is
   * `(doc) => Automerge.change(doc, ...)` or a `spliceText` call). Returns the
   * new live value.
   */
  edit(edit: (value: T) => T): T {
    this.#value = edit(this.#value)
    return this.#value
  }

  /**
   * Merge a concurrent sibling that arrived from sync into the live value, so
   * pending local edits survive and the next {@link RecordSession.encode} the
   * caller commits supersedes the sibling inclusively. Accepts either the raw
   * byte payload or an already-decoded value. An undecodable sibling is dropped
   * (returning `false`) rather than corrupting the live document.
   */
  absorb(sibling: Uint8Array | T): boolean {
    let incoming: T
    if (sibling instanceof Uint8Array) {
      try {
        incoming = this.#format.decode(sibling)
      } catch {
        return false
      }
    } else {
      incoming = sibling
    }
    this.#value = this.#format.merge(this.#value, incoming)
    return true
  }

  /** The canonical bytes of the live value — what a converging commit stores. */
  encode(): Uint8Array {
    return this.#format.encode(this.#value)
  }
}
