//! Automerge CRDT documents as Dialog record values — the JavaScript edge.
//
// The browser/worker mirror of the Rust `dialog-automerge` crate. It ships the
// `foldRecords` helper (the read half of the read-fold-edit-write discipline),
// the `RecordSession` doc-handle (the edit/absorb half), and the `TextDocument`
// automerge format binding. Only applications that hold automerge-typed record
// attributes import it — the Dialog core wasm blob takes no automerge
// dependency; stored record bytes are naked automerge `save()` output.

export type {
  RecordFormat,
  RecordSibling,
  RecordRow,
  FoldedRecord,
  FoldedGroup,
} from './record.js'
export { foldRecords, foldRecordGroups } from './record.js'

export { RecordSession } from './handle.js'

export { canonicalBytes } from './canonical.js'

export type { TextSchema } from './text.js'
export {
  TextDocument,
  createTextDocument,
  getText,
  spliceText,
  forkTextDocument,
  TEXT_KEY,
} from './text.js'

export * as Automerge from '@automerge/automerge'
