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

/**
 * We treate IPLD Link for empty byte array as an empty db revision.
 */
const REVISION = Link.of(null)
const GENESIS = REVISION.toString()

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
 * Represents a database revision using via IPLD link formatted as string.
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
  /**
   * DID identifier for the underlying database.
   */
  did(): DID

  /**
   * Takes changes and transacts them atomically into this database.
   */
  transact(changes: Changes): Task.Invocation<Revision, Error>

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

  /**
   * Closes session & cancels all the subscribers.
   */
  close(): void

  /**
   * Closes session and erases underlying store.
   */
  clear(): Task.Invocation<void, Error>
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
 * Open a session to a database identified by a gived DID.
 */
export const open = (did: DID) => DialogSession.open(did)

type Connection = Variant<{
  pending: Task.Invocation<Artifacts, Error>
  open: Artifacts
}>

/**
 * We hold weak references to db sessions to avoid having more than one
 * session for the same database in the same thread.
 */
const sessions = new Map<DID, WeakRef<DialogSession>>()

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
  static open(did: DID): Session {
    if (!did.startsWith('did:key:')) {
      throw new RangeError(`Only did:key identifiers are supported`)
    }

    const session = sessions.get(did)?.deref()
    if (session) {
      return session
    } else {
      const session = new this(did)
      sessions.set(did, new WeakRef(session))
      return session
    }
  }
  /**
   * Create a new ArtifactsStore instance
   */
  constructor(
    did: DID,
    subscriptions: Set<Subscription> = new Set(),
    channel = new BroadcastChannel(this.did())
  ) {
    this.#did = did
    this.#subscriptions = subscriptions
    this.#channel = channel
    this.#connection = { pending: Task.perform(DialogSession.connect(this)) }

    // DB may be mutated from the other sessions in order to know that we need
    // to rerun queries we subscribe to broadcast channel
    this.#channel.addEventListener('message', this)
  }

  #did
  #subscriptions
  #channel

  static *connect(self: DialogSession): Task.Task<Artifacts, Error> {
    if (ready === false) {
      ready = init()
      yield* Task.wait(ready)
      ready = true
    } else if (ready !== true) {
      yield* Task.wait(ready)
    }

    const conneciton = yield* Task.wait(Artifacts.open(self.did()))

    self.#connection = { open: conneciton }

    return conneciton
  }

  handleEvent(event: MessageEvent) {
    Task.perform(DialogSession.reset(this, event.data.revision ?? REVISION))
  }

  static *reset(self: DialogSession, revision: Revision) {
    const connection = yield* this.connected(self)
    // If we are resetting to genesis we can not actually reset because DB
    // was removed instead we reopen connection.
    if (revision === GENESIS) {
      self.#connection = { pending: Task.perform(this.connect(self)) }
      yield* this.connected(self)
    } else {
      yield* Task.wait(connection.reset())
    }
    yield* DialogSession.broadcast(self)
  }

  #connection: Connection

  static *connected(self: DialogSession): Task.Task<Artifacts, Error> {
    if (self.#connection.pending) {
      return yield* Task.wait(self.#connection.pending)
    } else {
      return self.#connection.open
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
  ): Task.Task<Revision, Error> {
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

    // We reset database before we commit in because IDB could have being updated
    yield* Task.wait(connection.reset())

    const revision = toRevision(
      yield* Task.wait(connection.commit(transaction))
    )

    // Notify other sessions to this db that we have commited some changes
    self.#channel.postMessage({ revision: revision.toString() })
    // We also broadcast changes to subscribers in this session.
    yield* this.broadcast(self)

    return revision
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
    if (sessions.get(this.did())?.deref() === this) {
      sessions.delete(this.did())
    }

    this.#channel.removeEventListener('message', this)
    this.#subscriptions.clear()
  }
  /**
   * Clears local replica for this database.
   */

  clear() {
    return Task.perform(DialogSession.clear(this))
  }

  static *revision(self: DialogSession) {
    const connection = yield* DialogSession.connected(self)

    return toRevision(yield* Task.wait(connection.revision()))
  }

  revision() {
    return Task.perform(DialogSession.revision(this))
  }

  static *clear(self: DialogSession) {
    self.close()
    // Wait if we're still connected before we dispose the database
    yield* this.connected(self)

    const erase = new Promise((resolve, reject) => {
      const request = indexedDB.deleteDatabase(self.did())
      request.onerror = reject
      request.onsuccess = resolve
    })

    yield* Task.wait(erase)

    // Tell other sessions that we have cleared the database
    self.#channel.postMessage({ revision: GENESIS })
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

/**
 * Takes bytes returned by the {@link Artifacts.commit} and derives an IPLD
 * link that we treat as revision hash.
 */

const toRevision = (bytes: Uint8Array) => Link.of(bytes).toString() as Revision
