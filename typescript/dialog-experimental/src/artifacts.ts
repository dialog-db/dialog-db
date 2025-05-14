import {
  Task,
  API,
  Constant,
  type Predicate,
  type FactSchema,
  type Querier,
  type FactsSelector,
  type Fact,
  type Datum,
  type The,
  type Scalar,
  type Assertion,
  type Variant,
  Instruction,
} from '@dialog-db/query'

import init, {
  Artifacts,
  ValueDataType,
  InstructionType,
  type Artifact,
  type ArtifactIterable,
  type ArtifactSelector,
} from './artifacts/dialog_artifacts.js'

let ready: true | false | Promise<unknown> = false

const { Link } = Constant
const ENTITY = Link.of(null)['/'].fill(0, 4)

const GENESIS = new Uint8Array(4 + 32 * 3)
GENESIS.fill(0)
GENESIS[0] = 1 // CID v1
GENESIS[1] = 0x55 // raw binary
GENESIS[2] = 0x00 // Identity multihash
GENESIS[3] = 32 * 3 // Size of the revision

const GENESIS_REVISION = Link.fromBytes(GENESIS)

const EMPTY_TREE = GENESIS_REVISION.toString() as Revision

/**
 * DID identifier.
 */
export type DID = `did:${string}:${string}`

/**
 * Change that retracts set of facts, which is usually a set corresponding to
 * one relation model.
 */
export interface Retraction extends Iterable<{ retract: Fact }> {}

/**
 * Change is either assertion or a rtercation.
 */
export type Change = Assertion | Retraction

/**
 * Changes are set of changes that can be transacted atomically.
 */
export interface Changes extends Iterable<Change> {}

/**
 * Represents a database revision using RAW identity CID.
 */
export interface Revision {
  toString(): string
}

/**
 * Subscriber is a function to be called with query results when some facts
 * are asserted or retracted.
 */
export type Subscriber<Fact> = (facts: Fact[]) => unknown

/**
 * Database session that can be used to query facts or/and transact changes.
 */
export interface Session extends Querier {
  did(): DID

  /**
   * Takes changes and transacts them atomically.
   */
  transact(changes: Changes): Task.Invocation<Session, Error>

  /**
   * Subscribes to the provided query & calls `subscriber` every time changes
   * are transacted in this session allowing subscribes to react to changes.
   * Retruns {@link Subscription} that can be used to either munally poll
   * session or to cancel subscription.
   */
  subscribe<Fact>(
    query: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ): Subscription
}

/**
 * Represents a query subscription that can be polled or cancelled.
 */
export interface Subscription {
  readonly cancelled: boolean
  cancel(): void
  poll(source: Session): Task.Task<{}, Error>
}

/**
 * Open an artifacts store with the specified address
 */
export const open = (did: DID) => DialogSession.open(did)

type Connection = Variant<{
  pending: Task.Invocation<Artifacts, Error>
  open: Artifacts
}>

/**
 * Implements a store for artifacts that provides querying and transaction capabilities
 * @implements {API.Querier}
 * @implements {API.Transactor}
 */
export class DialogSession implements Session {
  /**
   * Open an artifacts store
   * @param address The store address configuration
   * @returns A task that resolves to a Querier and Transactor interface
   */
  static open(did: DID) {
    if (!did.startsWith('did:key:')) {
      throw new RangeError(`Only did:key identifiers are supported`)
    }

    // We either use revision currently in stored in the localStorage or
    // start with an empty tree.
    const revision = localStorage.getItem(did) ?? EMPTY_TREE

    return new this(did, revision)
  }
  /**
   * Create a new ArtifactsStore instance
   */
  constructor(
    did: DID,
    revision: Revision,
    subscriptions: Set<Subscription> = new Set(),
    channel = new BroadcastChannel(this.did())
  ) {
    this.#did = did
    this.#revision = revision
    this.#subscriptions = subscriptions
    this.#channel = channel
    this.#connection = { pending: Task.perform(DialogSession.connect(this)) }

    this.#channel.addEventListener('message', this)
  }

  #did
  #revision
  #subscriptions
  #channel

  handleEvent(event: MessageEvent) {
    Task.perform(this.checkout())
  }

  /**
   * Reads revision for the given DID, if no revision info is found defaults to
   * the genesis revision.
   */
  static revision(did: DID) {
    return ((localStorage.getItem(did) as Revision) ?? EMPTY_TREE) as Revision
  }

  static *checkout(self: DialogSession) {
    // If connection is pending we wait, once it is established revision from
    // the tree will be used
    if (self.#connection.pending) {
      yield* Task.wait(this.connected(self))
    }

    // If we have connection but it's revision is out of date we got to
    // reconnect.
    if (self.#revision !== this.revision(self.did())) {
      self.#connection = { pending: Task.perform(this.connect(self)) }
      yield* this.connected(self)
    }

    // No we can broadcast changes to all the subscribers
    yield* this.broadcast(self)
  }
  /**
   * Checkout latest commited state of the database.
   */
  *checkout() {
    return Task.perform(DialogSession.checkout(this))
  }

  static *reset(self: DialogSession, revision: Revision) {
    // If desired revision is different we need to update local storage
    if (this.revision(self.did()) !== revision) {
      if (revision === EMPTY_TREE) {
        localStorage.removeItem(self.did())
      } else {
        localStorage.setItem(self.did(), revision.toString())
      }

      // Notify other sessions that revision has changed.
      self.#channel.postMessage({ revision })

      // And checkout db from the desired revision
      yield* this.checkout(self)
    }
  }
  /**
   * Resets database to the desired revision.
   */
  *reset(revision: Revision) {
    return Task.perform(DialogSession.reset(this, revision))
  }

  #connection: Connection

  static *connected(self: DialogSession): Task.Task<Artifacts, Error> {
    if (self.#connection.pending) {
      return yield* Task.wait(self.#connection.pending)
    } else {
      return self.#connection.open
    }
  }

  static *connect(self: DialogSession): Task.Task<Artifacts, Error> {
    if (ready === false) {
      ready = init()
      yield* Task.wait(ready)
      ready = true
    } else if (ready !== true) {
      yield* Task.wait(ready)
    }

    while (true) {
      const revision = this.revision(self.did())
      const conneciton = yield* Task.wait(
        Artifacts.open(
          self.did(),
          // If revision is different from the base tree we
          revision === EMPTY_TREE ? undefined : encodeRevision(revision)
        )
      )

      self.#revision = revision
      if (this.revision(self.did()) === revision) {
        self.#connection = { open: conneciton }
        return conneciton
      }
    }
  }

  /**
   * Returns DID identifier for this database.
   */
  did() {
    return this.#did
  }

  /**
   * Select artifacts that match the given selector
   * @param selector The selection criteria
   * @returns A task that resolves to matching artifacts
   */
  select(selector: FactsSelector) {
    return Task.perform(DialogSession.select(this, selector))
  }

  /**
   * Execute a transaction to update the store
   * @param transaction The transaction to apply
   * @returns A task that resolves to this store instance
   */
  transact(changes: Changes) {
    return Task.perform(DialogSession.transact(this, changes))
  }

  /**
   * Select artifacts from the store
   * @param self The store instance
   * @param selector The selection criteria
   * @returns A task generator that yields the selected artifacts
   */
  static *select(self: DialogSession, selector: FactsSelector) {
    const connection = yield* this.connected(self)
    const matches = yield* Task.wait(
      connection.select({
        the: selector.the ? selector.the : undefined,
        of: selector.of ? toEntity(selector.of) : undefined,
        is: selector.is ? toTyped(selector.is) : undefined,
      })
    )

    return yield* Task.wait(fromIterable(matches))
  }

  /**
   * Apply a changes to the undelying store.
   */
  static *transact(
    self: DialogSession,
    changes: Changes
  ): Task.Task<DialogSession, Error> {
    const transaction = []
    for (const { assert, retract } of instructions(changes)) {
      if (assert) {
        transaction.push({
          type: InstructionType.Assert,
          artifact: toArtifact(assert),
        })
      }
      if (retract) {
        transaction.push({
          type: InstructionType.Retract,
          artifact: toArtifact(retract),
        })
      }
    }

    const connection = yield* this.connected(self)
    yield* Task.wait(connection.commit(transaction))

    const bytes = yield* Task.wait(connection.revision())
    const revision = decodeRevision(bytes!)

    self.#revision = revision
    localStorage.setItem(self.did(), revision.toString())

    // Notify other sessions that data has changed.
    self.#channel.postMessage({ revision })
    yield* this.broadcast(self)

    return self
  }

  static *broadcast(self: DialogSession) {
    const subscriptions = self.#subscriptions
    for (const subscription of subscriptions) {
      if (subscription.cancelled) {
        subscriptions.delete(subscription)
      } else {
        const result = yield* Task.result(subscription.poll(self))
        if (result.error) {
          console.error(result.error)
        }
      }
    }
  }

  /**
   * Subscribes to the querable predicate with a provided subscriber and will
   * call subscriber with new query results when new changes are transacted.
   */

  subscribe<Fact>(
    predicate: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ) {
    return DialogSession.subscribe(this, predicate, subscriber)
  }

  static subscribe<Fact>(
    self: DialogSession,
    query: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ) {
    const subscription = new QuerySubscription(query, subscriber)
    self.#subscriptions.add(subscription)
    return subscription
  }

  close() {
    this.#subscriptions.clear()
  }
  /**
   * Clears local replica for this database.
   */

  clear() {
    return Task.perform(DialogSession.clear(this))
  }
  static *clear(self: DialogSession) {
    yield* this.connected(self)
    self.close()

    const erase = new Promise((resolve, reject) => {
      const request = indexedDB.deleteDatabase(self.did())
      request.onerror = reject
      request.onsuccess = resolve
    })
    yield* Task.wait(erase)
    yield* this.reset(self, EMPTY_TREE)
  }
}

class QuerySubscription<Fact> implements Subscription {
  #cancelled = false
  constructor(
    public predicate: Predicate<Fact, string, FactSchema>,
    public subscriber: (facts: Fact[]) => unknown
  ) {
    this.cancel = this.cancel.bind(this)
  }

  get cancelled() {
    return this.#cancelled
  }

  *poll(source: Querier) {
    if (!this.#cancelled) {
      const facts = yield* this.predicate.query({ from: source })
      this.subscriber(facts)
    }
    return {}
  }

  cancel() {
    this.#cancelled = true
  }
}

/**
 * Convert a fact to an artifact
 * @param fact The fact to convert
 * @returns The corresponding artifact
 */
const toArtifact = ({ the, of, is }: Fact): Artifact => ({
  the,
  of: toEntity(of),
  is: toTyped(is),
})

/**
 * Convert an artifact to a datum
 * @param artifact The artifact to convert
 * @returns The corresponding datum
 */
const fromArtifact = ({ the, of, is }: Artifact): Datum => ({
  the: the as The,
  of: fromEntity(of),
  is: is.value,
  cause: Link.of({ the, of, is: is.value }),
})

const fromIterable = async (iterable: ArtifactIterable) => {
  const selection = []
  for await (const entry of iterable) {
    selection.push(fromArtifact(entry))
  }

  return selection
}

const select = async (connection: Artifacts, selector: ArtifactSelector) => {}
/**
 * Convert a link to an entity
 * @param link The link to convert
 * @returns The entity bytes
 */
const toEntity = (link: API.Link): Uint8Array => link['/'].subarray(-32)

/**
 * Convert an entity to a link
 * @param entity The entity bytes
 * @returns The corresponding link
 */
const fromEntity = (entity: Uint8Array): API.Link => {
  ENTITY.set(entity, 4)
  return Link.fromBytes(ENTITY.slice(0))
}

/**
 * Convert a scalar value to a typed value
 * @param value The scalar value to convert
 * @returns The typed value
 */
const toTyped = (
  value: Scalar
): {
  type: ValueDataType
  value: null | Uint8Array | string | boolean | number
} => {
  switch (typeof value) {
    case 'boolean':
      return { type: ValueDataType.Boolean, value }
    case 'number': {
      return (
        Number.isInteger(value) ? { value, type: ValueDataType.SignedInt }
        : Number.isFinite(value) ? { value, type: ValueDataType.Float }
        : unreachable(`Number ${value} can not be inferred`)
      )
    }
    case 'bigint': {
      return { type: ValueDataType.SignedInt, value: Number(value) }
    }
    case 'string': {
      return { type: ValueDataType.String, value }
    }
    default: {
      if (value instanceof Uint8Array) {
        return { type: ValueDataType.Bytes, value }
      } else if (Link.is(value)) {
        return { type: ValueDataType.Entity, value: value['/'] }
      } else if (value === null) {
        return { type: ValueDataType.Null, value }
      } else {
        throw Object.assign(new TypeError(`Object types are not supported`), {
          value,
        })
      }
    }
  }
}

/**
 * Function for handling unreachable code paths
 * @param message Error message
 * @returns Never returns
 */
export const unreachable = (message: string): never => {
  throw new Error(message)
}

function* instructions(
  changes: Iterable<Assertion | Retraction>
): Iterable<Instruction> {
  for (const change of changes) {
    yield* change
  }
}

const encodeRevision = (revision: Revision) =>
  Link.fromJSON({ '/': revision.toString() })['/'].subarray(4)

const decodeRevision = (bytes: Uint8Array) => {
  const REVISION = GENESIS_REVISION['/'].slice(0)
  REVISION.set(bytes, 4)
  return Link.fromBytes(REVISION).toString() as Revision
}
