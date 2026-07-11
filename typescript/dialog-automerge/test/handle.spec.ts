import assert from 'node:assert/strict'

import { RecordSession } from '../src/handle.js'
import {
  createTextDocument,
  spliceText,
  forkTextDocument,
  getText,
  TextDocument,
} from '../src/text.js'

const encode = TextDocument.encode

describe('RecordSession', () => {
  it('opens on the fold of a diverged group and reports it', () => {
    let base = createTextDocument()
    base = spliceText(base, 0, 0, 'draft')
    let left = forkTextDocument(base)
    let right = forkTextDocument(base)
    left = spliceText(left, 0, 0, 'my ')
    right = spliceText(right, 5, 0, ' notes')

    const opened = RecordSession.open(TextDocument, [encode(left), encode(right)])
    assert.ok(opened)
    assert.ok(opened.session)
    assert.equal(opened.folded.siblings, 2)
    assert.equal(getText(opened.session.value), 'my draft notes')
  })

  it('returns undefined for an empty group', () => {
    assert.equal(RecordSession.open(TextDocument, []), undefined)
  })

  it('opens without a session when every sibling is undecodable', () => {
    const opened = RecordSession.open(TextDocument, [
      new Uint8Array([0x01]),
      new Uint8Array([0x02]),
    ])
    assert.ok(opened)
    assert.equal(opened.session, undefined)
    assert.equal(opened.folded.value, undefined)
  })

  it('applies local edits to the live value', () => {
    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'hello')
    const session = new RecordSession(TextDocument, document)

    session.edit((doc) => spliceText(doc, 5, 0, ' world'))
    assert.equal(getText(session.value), 'hello world')
  })

  it('absorbs a mid-session sibling without losing pending local edits (§6.15)', () => {
    // Shared starting point, replicated to two replicas.
    let base = createTextDocument()
    base = spliceText(base, 0, 0, 'shared')

    // This replica opens a session and starts editing locally.
    const session = new RecordSession(TextDocument, forkTextDocument(base))
    session.edit((doc) => spliceText(doc, 0, 0, '>> '))

    // Meanwhile another replica edits concurrently; its sibling arrives via sync.
    let other = forkTextDocument(base)
    other = spliceText(other, 6, 0, ' world')
    const absorbed = session.absorb(encode(other))

    assert.equal(absorbed, true)
    // The next commit is inclusive of both sides' edits — no lost edit.
    const committed = TextDocument.decode(session.encode())
    assert.equal(getText(committed), '>> shared world')
  })

  it('drops an undecodable arrival rather than corrupting the live value', () => {
    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'intact')
    const session = new RecordSession(TextDocument, document)

    const absorbed = session.absorb(new Uint8Array([0xde, 0xad]))
    assert.equal(absorbed, false)
    assert.equal(getText(session.value), 'intact')
  })

  it('encodes the live value to canonical bytes', () => {
    let document = createTextDocument()
    document = spliceText(document, 0, 0, 'canonical')
    const session = new RecordSession(TextDocument, document)

    assert.deepEqual(session.encode(), encode(document))
  })
})
