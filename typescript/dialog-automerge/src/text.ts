import * as Automerge from '@automerge/automerge'

import { canonicalBytes } from './canonical.js'
import type { RecordFormat } from './record.js'

/** The root map key under which the document's text lives — the same key the
 * Rust `TextDocument` uses, so the two decode each other's bytes. */
export const TEXT_KEY = 'text'

/** The document shape: a single collaborative text field at the root. */
export interface TextSchema {
  text: string
}

/** A collaborative text document backed by an automerge CRDT, carried by Dialog
 * as opaque canonical bytes. Concurrent edits on different replicas converge
 * via {@link RecordFormat.merge} instead of surfacing as divergent siblings. */
export type TextDocument = Automerge.Doc<TextSchema>

/**
 * Create a new, empty text document.
 *
 * This mints the document's root text field and therefore its identity:
 * replicas that should converge must descend from one created document (via a
 * storage round-trip or {@link forkTextDocument}), not from independent calls
 * to `createTextDocument` (see the crate docs on shared ancestry).
 */
export function createTextDocument(): TextDocument {
  return Automerge.change(Automerge.init<TextSchema>(), (doc) => {
    doc.text = ''
  })
}

/** The current text content of the document. */
export function getText(document: TextDocument): string {
  return document.text
}

/**
 * Splice `insert` into the text at `position`, first deleting `remove`
 * characters. Returns the edited document (automerge documents are immutable).
 */
export function spliceText(
  document: TextDocument,
  position: number,
  remove: number,
  insert: string
): TextDocument {
  return Automerge.change(document, (doc) => {
    Automerge.splice(doc, [TEXT_KEY], position, remove, insert)
  })
}

/** Fork the document into an independent replica with its own actor id. The
 * fork shares this document's history, so edits on either side converge when
 * the two are merged. */
export function forkTextDocument(document: TextDocument): TextDocument {
  return Automerge.clone(document)
}

/**
 * The {@link RecordFormat} for automerge text documents — the JS mirror of the
 * Rust `TextDocument`. `decode`/`encode`/`merge` agree with the native crate so
 * a document is interchangeable across the boundary.
 */
export const TextDocument: RecordFormat<TextDocument> = {
  decode(bytes: Uint8Array): TextDocument {
    const document = Automerge.load<TextSchema>(bytes)
    // Shape gate for foreign bytes: any loadable document is valid automerge,
    // but only one carrying a text field at the root is a text document.
    // Rejecting here lets the read-side fold drop malformed siblings.
    if (typeof (document as { text?: unknown }).text !== 'string') {
      throw new Error(`automerge document has no text at root key ${JSON.stringify(TEXT_KEY)}`)
    }
    return document
  },

  encode(document: TextDocument): Uint8Array {
    // Canonical form: the bytes are a pure function of the change-set,
    // independent of merge order and of any compression library.
    return canonicalBytes(document)
  },

  merge(a: TextDocument, b: TextDocument): TextDocument {
    try {
      return Automerge.merge(Automerge.clone(a), b)
    } catch {
      // Merge of two successfully-loaded documents failing means corrupt
      // internal state; degrade to the deterministic default (`b` wins) rather
      // than poisoning the read path.
      return Automerge.clone(b)
    }
  },
}
