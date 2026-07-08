import * as Automerge from '@automerge/automerge'

/**
 * Encode an automerge document to bytes that are a pure function of its
 * change-set: any two documents holding the same changes — whatever order they
 * merged them in — encode byte-identically.
 *
 * This is the JavaScript mirror of `dialog-automerge`'s Rust `canonical_bytes`
 * (WS3-core). A record value is stored, keyed, and compared by its bytes, so a
 * document written by a browser replica and a document written by a native
 * replica for the same edits must mint the identical value — and therefore the
 * identical tree key. Automerge's own {@link Automerge.save} does not provide
 * this on its own: it encodes changes in the order they entered the local
 * change graph, and for *concurrent* changes that order depends on the order
 * the document merged them.
 *
 * The algorithm dispatches on a property of the change-set itself, so both the
 * path taken and its output match on every replica:
 *
 * - A *linear* history — a single head, no change with more than one
 *   dependency — has exactly one topological order, which insertion order must
 *   equal; the document's own `save` is already canonical. This is the
 *   never-diverged common case and costs nothing extra.
 * - Otherwise the document is rebuilt by applying its changes in canonical
 *   order — a topological sort of the change DAG, smallest change hash first
 *   among the concurrently-ready — at a cost proportional to the full history,
 *   paid only by documents whose history has ever diverged.
 *
 * Compression is deliberately not applied: DEFLATE output is not specified
 * across compressor versions, so compressed bytes cannot serve as identity
 * (the `deflate: false` half of Rust's `canonical_options`). Automerge's `save`
 * only compresses chunks above an internal threshold, so canonicalizing
 * through a rebuild — which re-derives the columnar encoding from scratch —
 * keeps identity a function of automerge's own encoder alone. Size is
 * reclaimed below the identity layer, the same arrangement git uses: hash
 * uncompressed, compress at rest.
 *
 * Canonical output is stable per automerge major version, not across majors:
 * every participant that writes record bytes must pin the same automerge
 * version (see {@link https://github.com/dialog-db/dialog-db/blob/main/notes/automerge-integration-spec.md | the spec}, §6.7).
 */
export function canonicalBytes(document: Automerge.Doc<unknown>): Uint8Array {
  const changes = Automerge.getAllChanges(document)
  const decoded = changes.map((change) => Automerge.decodeChange(change))

  const linear =
    Automerge.getHeads(document).length === 1 &&
    decoded.every((change) => change.deps.length <= 1)
  if (linear) {
    return Automerge.save(document)
  }

  const order = canonicalOrder(decoded)
  let rebuilt = Automerge.init<unknown>()
  ;[rebuilt] = Automerge.applyChanges(
    rebuilt,
    order.map((index) => changes[index])
  )
  return Automerge.save(rebuilt)
}

/**
 * The canonical order of `changes`, as indices into the array: a topological
 * sort of the dependency DAG that breaks ties between concurrently-ready
 * changes by their (content-addressed) hash.
 *
 * Automerge exposes change hashes as fixed-width hex strings, so a lexicographic
 * string comparison orders them identically to the byte-lexicographic ordering
 * the Rust side applies to `ChangeHash` — the two sides break ties the same way.
 */
function canonicalOrder(changes: Automerge.DecodedChange[]): number[] {
  const indexByHash = new Map(changes.map((change, index) => [change.hash, index]))

  const blocking = changes.map(() => 0)
  const dependents: number[][] = changes.map(() => [])
  changes.forEach((change, index) => {
    for (const dependency of change.deps) {
      const source = indexByHash.get(dependency)
      if (source === undefined) {
        throw new Error('change depends on a change outside the document')
      }
      blocking[index] += 1
      dependents[source].push(index)
    }
  })

  // A min-ordered set of ready changes keyed by hash; `insert` keeps it sorted
  // so `shift` always yields the smallest-hash ready change.
  const ready: Array<[string, number]> = []
  const insert = (entry: [string, number]) => {
    let low = 0
    let high = ready.length
    while (low < high) {
      const mid = (low + high) >>> 1
      if (ready[mid][0] < entry[0]) low = mid + 1
      else high = mid
    }
    ready.splice(low, 0, entry)
  }
  changes.forEach((change, index) => {
    if (blocking[index] === 0) insert([change.hash, index])
  })

  const order: number[] = []
  let next: [string, number] | undefined
  while ((next = ready.shift()) !== undefined) {
    const [, index] = next
    order.push(index)
    for (const dependent of dependents[index]) {
      blocking[dependent] -= 1
      if (blocking[dependent] === 0) {
        insert([changes[dependent].hash, dependent])
      }
    }
  }

  if (order.length !== changes.length) {
    throw new Error('dependency cycle among the document’s changes')
  }
  return order
}
