import assert from 'node:assert/strict'
import * as Automerge from '@automerge/automerge'

import { canonicalBytes } from '../src/canonical.js'
import { createTextDocument, spliceText, forkTextDocument, TextDocument } from '../src/text.js'

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString('hex')

describe('canonicalBytes', () => {
  it('is byte-identical whichever way a diverged history merged', () => {
    let base = createTextDocument()
    base = spliceText(base, 0, 0, 'shared')

    let left = forkTextDocument(base)
    let right = forkTextDocument(base)
    left = spliceText(left, 0, 0, 'L')
    right = spliceText(right, 6, 0, 'R')

    const leftFirst = TextDocument.merge(left, right)
    const rightFirst = TextDocument.merge(right, left)

    // Automerge's own save is order-dependent for concurrent changes...
    assert.notEqual(hex(Automerge.save(leftFirst)), hex(Automerge.save(rightFirst)))
    // ...but the canonical form is not.
    assert.equal(hex(canonicalBytes(leftFirst)), hex(canonicalBytes(rightFirst)))
  })

  it('is stable across a decode/re-encode round trip', () => {
    let base = createTextDocument()
    base = spliceText(base, 0, 0, 'draft')
    let left = forkTextDocument(base)
    let right = forkTextDocument(base)
    left = spliceText(left, 0, 0, 'my ')
    right = spliceText(right, 5, 0, ' notes')

    const merged = TextDocument.merge(left, right)
    const bytes = canonicalBytes(merged)
    const reloaded = Automerge.load(bytes)

    assert.equal(hex(canonicalBytes(reloaded)), hex(bytes))
  })

  it('agrees between the linear fast path and a from-scratch rebuild', () => {
    // A strictly linear history: the document's own save must equal the
    // canonical rebuild. Force the rebuild path by feeding the changes back in
    // through a merge with an empty fork (which keeps the history linear here).
    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'a strictly linear history')
    document = spliceText(document, 0, 0, 'with ')

    // The fast path is what canonicalBytes takes for this document.
    const fast = canonicalBytes(document)

    // Rebuild from scratch in canonical order and compare.
    const changes = Automerge.getAllChanges(document)
    const decoded = changes.map((change) => Automerge.decodeChange(change))
    // Sanity: this history really is linear.
    assert.ok(decoded.every((change) => change.deps.length <= 1))

    let rebuilt = Automerge.init<unknown>()
    ;[rebuilt] = Automerge.applyChanges(rebuilt, changes)
    assert.equal(hex(fast), hex(Automerge.save(rebuilt)))
  })

  it('encodes documents with no shared ancestry identically across fold order', () => {
    // Concurrent root changes permute the change hashes canonical ordering
    // tie-breaks on; repeat to exercise many random actor-id draws.
    for (let round = 0; round < 32; round += 1) {
      let one = createTextDocument()
      let other = createTextDocument()
      one = spliceText(one, 0, 0, 'one')
      other = spliceText(other, 0, 0, 'other')

      const ab = canonicalBytes(TextDocument.merge(one, other))
      const ba = canonicalBytes(TextDocument.merge(other, one))
      assert.equal(hex(ab), hex(ba))
    }
  })

  it('encodes an empty document deterministically and round-trips it', () => {
    const document = Automerge.init<unknown>()
    const bytes = canonicalBytes(document)
    const reloaded = Automerge.load(bytes)
    assert.equal(hex(canonicalBytes(reloaded)), hex(bytes))
  })
})
