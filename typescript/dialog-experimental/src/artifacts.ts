import {
  Task,
  API,
  Constant,
  type Predicate,
  type FactSchema,
  type Querier,
  type Transactor,
  type Transaction,
  type FactsSelector,
  type Fact,
  type Datum,
  type The,
  type Scalar,
} from '@dialog-db/query'

import init, {
  Artifacts,
  ValueDataType,
  InstructionType,
  type Artifact,
} from './artifacts/dialog_artifacts.js'

let initialized = false

const { Link } = Constant
const ENTITY = Link.of(null)['/'].fill(0, 4)

/**
 * Address configuration for opening an artifact store
 */
export type Address = {
  name: string
  revision?: Uint8Array
}

export type Subscriber<Fact> = (facts: Fact[]) => unknown

export interface Source extends Querier {
  subscribe<Fact>(
    query: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ): Subscription
}

export interface Subscription {
  readonly cancelled: boolean
  cancel(): void
  poll(source: Source): Task.Task<{}, Error>
}

/**
 * Open an artifacts store with the specified address
 * @param address The store address configuration
 * @returns A task that resolves to a Querier and Transactor interface
 */
export const open = (address: Address) =>
  Task.perform(ArtifactsStore.open(address))

const GENESYS = new Uint8Array()

/**
 * Implements a store for artifacts that provides querying and transaction capabilities
 * @implements {API.Querier}
 * @implements {API.Transactor}
 */
export class ArtifactsStore implements Querier, Transactor, Source {
  /**
   * Open an artifacts store
   * @param address The store address configuration
   * @returns A task that resolves to a Querier and Transactor interface
   */
  static *open(address: Address) {
    if (!initialized) {
      yield* Task.wait(init())
      initialized = true
    }

    const instance = yield* Task.wait(
      Artifacts.open(address.name, address.revision)
    )
    const revision = yield* Task.wait(instance.revision())

    return new this(instance, revision ?? GENESYS)
  }

  static subscribe<Fact>(
    self: ArtifactsStore,
    query: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ) {
    const subscription = new QuerySubscription(query, subscriber)
    self.subscriptions.add(subscription)
    return subscription
  }

  /**
   * Create a new ArtifactsStore instance
   * @param instance The underlying artifacts instance
   * @param revision The current revision
   */
  constructor(
    public instance: Artifacts,
    private revision: Uint8Array,
    public subscriptions: Set<Subscription> = new Set()
  ) {}

  /**
   * Select artifacts that match the given selector
   * @param selector The selection criteria
   * @returns A task that resolves to matching artifacts
   */
  select(selector: FactsSelector) {
    return Task.perform(ArtifactsStore.select(this, selector))
  }

  /**
   * Execute a transaction to update the store
   * @param transaction The transaction to apply
   * @returns A task that resolves to this store instance
   */
  transact(transaction: Transaction) {
    return Task.perform(ArtifactsStore.transact(this, transaction))
  }

  /**
   * Select artifacts from the store
   * @param self The store instance
   * @param selector The selection criteria
   * @returns A task generator that yields the selected artifacts
   */
  static *select(self: ArtifactsStore, selector: FactsSelector) {
    const matches = yield* Task.wait(
      self.instance.select({
        the: selector.the ? selector.the : undefined,
        of: selector.of ? toEntity(selector.of) : undefined,
        is: selector.is ? toTyped(selector.is) : undefined,
      })
    )

    const iterator = matches[Symbol.asyncIterator]()
    const selection = []
    while (true) {
      const entry = yield* Task.wait(iterator.next())
      if (entry.done) {
        break
      } else {
        selection.push(fromArtifact(entry.value))
      }
    }

    return selection
  }

  /**
   * Apply a transaction to the store
   * @param self The store instance
   * @param transaction The transaction to apply
   * @returns A task generator that yields the updated store
   */
  static *transact(self: ArtifactsStore, transaction: Transaction) {
    const changes = []
    for (const { assert, retract } of transaction) {
      if (assert) {
        changes.push({
          type: InstructionType.Assert,
          artifact: toArtifact(assert),
        })
      }
      if (retract) {
        changes.push({
          type: InstructionType.Retract,
          artifact: toArtifact(retract),
        })
      }
    }

    yield* Task.wait(self.instance.commit(changes))

    yield* this.broadcast(self)

    const revision = yield* Task.wait(self.instance.revision())
    if (revision) {
      self.revision = revision
    }

    return self
  }

  static *broadcast(self: ArtifactsStore) {
    const { subscriptions } = self
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

  subscribe<Fact>(
    predicate: Predicate<Fact, string, FactSchema>,
    subscriber: Subscriber<Fact>
  ) {
    return ArtifactsStore.subscribe(this, predicate, subscriber)
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
