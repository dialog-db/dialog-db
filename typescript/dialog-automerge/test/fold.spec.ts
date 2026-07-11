import assert from 'node:assert/strict'
import * as Automerge from '@automerge/automerge'

import { foldRecords, foldRecordGroups } from '../src/record.js'
import type { RecordRow } from '../src/record.js'
import {
  createTextDocument,
  spliceText,
  forkTextDocument,
  getText,
  TextDocument,
} from '../src/text.js'

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString('hex')
const encode = (document: Automerge.Doc<{ text: string }>): Uint8Array =>
  TextDocument.encode(document)

/** A base document, then two divergent siblings whose edits do not overlap. */
const divergedSiblings = (): { left: Uint8Array; right: Uint8Array } => {
  let base = createTextDocument()
  base = spliceText(base, 0, 0, 'draft')
  let left = forkTextDocument(base)
  let right = forkTextDocument(base)
  left = spliceText(left, 0, 0, 'my ')
  right = spliceText(right, 5, 0, ' notes')
  return { left: encode(left), right: encode(right) }
}

describe('foldRecords', () => {
  it('returns undefined for an empty group', () => {
    assert.equal(foldRecords([], TextDocument), undefined)
  })

  it('passes a single sibling through without merging', () => {
    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'lonely')
    const bytes = encode(document)

    const folded = foldRecords([bytes], TextDocument)
    assert.ok(folded)
    assert.equal(folded.siblings, 1)
    assert.equal(folded.undecodable, 0)
    assert.equal(getText(folded.value!), 'lonely')
    assert.equal(hex(folded.bytes), hex(bytes))
  })

  it('folds diverged siblings into one value that keeps both edits', () => {
    const { left, right } = divergedSiblings()
    const folded = foldRecords([left, right], TextDocument)
    assert.ok(folded)
    assert.equal(folded.siblings, 2)
    assert.equal(getText(folded.value!), 'my draft notes')
  })

  it('is order-insensitive: both fold orders mint the identical value', () => {
    const { left, right } = divergedSiblings()
    const one = foldRecords([left, right], TextDocument)!
    const other = foldRecords([right, left], TextDocument)!
    assert.equal(hex(one.bytes), hex(other.bytes))
  })

  it('accepts query rows exposing bytes at is.value', () => {
    const { left, right } = divergedSiblings()
    const folded = foldRecords(
      [{ is: { value: left } }, { is: { value: right } }],
      TextDocument
    )
    assert.ok(folded)
    assert.equal(getText(folded.value!), 'my draft notes')
  })

  it('is monotone: folding a stale sibling into a fresh one is harmless', () => {
    let base = createTextDocument()
    base = spliceText(base, 0, 0, 'v1')
    const stale = encode(base)
    const ahead = encode(spliceText(base, 2, 0, ' v2'))

    const folded = foldRecords([stale, ahead], TextDocument)!
    assert.equal(hex(folded.bytes), hex(ahead))
  })

  it('drops an undecodable sibling and folds the rest (§6.9)', () => {
    const { left, right } = divergedSiblings()
    const garbage = new Uint8Array([0xde, 0xad, 0xbe, 0xef])

    const folded = foldRecords([left, garbage, right], TextDocument)!
    assert.equal(folded.siblings, 3)
    assert.equal(folded.undecodable, 1)
    assert.equal(getText(folded.value!), 'my draft notes')
  })

  it('never fails the read when every sibling is undecodable (§6.9)', () => {
    const a = new Uint8Array([0x01, 0x02])
    const b = new Uint8Array([0x00, 0xff])
    const folded = foldRecords([a, b], TextDocument)!
    assert.equal(folded.value, undefined)
    assert.equal(folded.undecodable, 2)
    // Deterministic raw winner: the lexicographically smallest payload.
    assert.equal(hex(folded.bytes), hex(b))
  })

  it('rejects a well-formed automerge doc that is not a text document', () => {
    // A loadable automerge document with no `text` field is valid automerge but
    // not a TextDocument; it must be dropped, not merged.
    let foreign = Automerge.init<{ title: string }>()
    foreign = Automerge.change(foreign, (doc) => {
      doc.title = 'not a text document'
    })
    const foreignBytes = Automerge.save(foreign)

    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'real')
    const realBytes = encode(document)

    const folded = foldRecords([realBytes, foreignBytes], TextDocument)!
    assert.equal(folded.undecodable, 1)
    assert.equal(getText(folded.value!), 'real')
  })
})

describe('foldRecordGroups', () => {
  it('folds each (the, of) group independently, one value per key', () => {
    const { left, right } = divergedSiblings()

    let solo = createTextDocument()
    solo = spliceText(solo, 0, 0, 'solo')
    const soloBytes = encode(solo)

    const entityA = new Uint8Array([1, 1, 1, 1])
    const entityB = new Uint8Array([2, 2, 2, 2])

    const rows: RecordRow[] = [
      { the: 'note/body', of: entityA, is: { value: left } },
      { the: 'note/body', of: entityB, is: { value: soloBytes } },
      { the: 'note/body', of: entityA, is: { value: right } },
    ]

    const groups = foldRecordGroups(rows, TextDocument)
    assert.equal(groups.length, 2)

    const byEntity = new Map(groups.map((g) => [hex(g.of), g]))
    assert.equal(getText(byEntity.get(hex(entityA))!.folded.value!), 'my draft notes')
    assert.equal(byEntity.get(hex(entityA))!.folded.siblings, 2)
    assert.equal(getText(byEntity.get(hex(entityB))!.folded.value!), 'solo')
    assert.equal(byEntity.get(hex(entityB))!.folded.siblings, 1)
  })

  it('separates groups that share an entity but differ in attribute', () => {
    const entity = new Uint8Array([9, 9, 9, 9])
    const body = encode(spliceText(createTextDocument(), 0, 0, 'body'))
    const title = encode(spliceText(createTextDocument(), 0, 0, 'title'))

    const rows: RecordRow[] = [
      { the: 'note/body', of: entity, is: { value: body } },
      { the: 'note/title', of: entity, is: { value: title } },
    ]

    const groups = foldRecordGroups(rows, TextDocument)
    assert.equal(groups.length, 2)
  })
})
