import { Task, API, Constant } from '@dialog-db/query'

import init, {
  Artifacts,
  ValueDataType,
  InstructionType,
} from './artifacts/dialog_artifacts.js'
import * as ArtifactsLib from './artifacts/dialog_artifacts.js'

let initialized = false

const { Link } = Constant
const ENTITY = Link.of(null)['/'].fill(0, 4)

/**
 * @typedef {ArtifactsLib.Artifact} Artifact
 */

/**
 * @typedef {object} Address
 * @property {string} name
 * @property {Uint8Array} [revision]
 * @param {Address} address
 * @returns {API.Task<API.Querier & API.Transactor, Error>}
 */
export const open = (address) => Task.perform(ArtifactsStore.open(address))

const GENESYS = new Uint8Array()

/**
 * @implements {API.Querier}
 * @implements {API.Transactor}
 */
class ArtifactsStore {
  /**
   * @param {Address} address
   * @returns {API.Task<API.Querier & API.Transactor, Error>}
   */
  static *open(address) {
    if (!initialized) {
      yield* Task.wait(init())
      initialized = true
    }

    const artifact = yield* Task.wait(
      Artifacts.open(address.name, address.revision)
    )
    const revision = yield* Task.wait(artifact.revision())

    return new ArtifactsStore(artifact, revision ?? GENESYS)
  }
  /**
   * @param {Artifacts} instance
   * @param {Uint8Array} revision
   */
  constructor(instance, revision) {
    this.artifacts = instance
    this.revision = revision
  }

  /**
   * * @param {API.FactsSelector} selector
   */
  select(selector) {
    return Task.perform(ArtifactsStore.select(this, selector))
  }

  /**
   * @param {API.Transaction} transaction
   */

  transact(transaction) {
    return Task.perform(ArtifactsStore.transact(this, transaction))
  }

  /**
   * @param {ArtifactsStore} self
   * @param {API.FactsSelector} selector
   */
  static *select(self, selector) {
    const matches = yield* Task.wait(
      self.artifacts.select({
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
   * @param {ArtifactsStore} self
   * @param {API.Transaction} transaction
   */
  static *transact(self, transaction) {
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

    yield* Task.wait(self.artifacts.commit(changes))

    const revision = yield* Task.wait(self.artifacts.revision())
    if (revision) {
      self.revision = revision
    }

    return self
  }
}

/**
 * @param {API.Fact} fact
 * @returns {Artifact}
 */

const toArtifact = ({ the, of, is }) => ({
  the,
  of: toEntity(of),
  is: toTyped(is),
})

/**
 * @param {Artifact} artifact
 * @returns {API.Datum}
 */
const fromArtifact = ({ the, of, is }) => ({
  the: /** @type {API.The} */ (the),
  of: fromEntity(of),
  is: is.value,
  cause: Link.of({ the, of, is: is.value }),
})

/**
 * @param {API.Link} link
 */
const toEntity = (link) => link['/'].subarray(-32)

/**
 *
 * @param {Uint8Array} entity
 * @returns {API.Link}
 */
const fromEntity = (entity) => (
  ENTITY.set(entity, 4), Link.fromBytes(ENTITY.slice(0))
)

/**
 *
 * @param {API.Scalar} value
 * @returns {{type: ValueDataType, value: null|Uint8Array|string|boolean|number}}
 */
const toTyped = (value) => {
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
 * @param {string} message
 * @returns {never}
 */
export const unreachable = (message) => {
  throw new Error(message)
}
