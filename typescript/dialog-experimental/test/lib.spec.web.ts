import { Artifacts, Query, fact, Task } from './self.js'
import { assert } from '@open-wc/testing'
import { alice, bob } from './constants.js'

describe('experimental', () => {
  it('imports the module', () => {
    assert.equal(typeof Artifacts, 'object')
    assert.equal(typeof Query, 'object')
  })

  it('can perform basic transactions', async () =>
    Task.spawn(function* () {
      const Counter = fact({
        name: String,
        value: Number,
      })

      const db = Artifacts.open(alice)

      yield* db.transact([Counter.assert({ name: 'test', value: 0 })])

      const results = yield* Counter().query({ from: db })

      assert.deepEqual(results, [Counter.assert({ name: 'test', value: 0 })])

      yield* db.transact([Counter.assert({ name: 'test', value: 5 })])

      assert.deepEqual(
        yield* Counter().query({ from: db }),
        [
          Counter.assert({ name: 'test', value: 0 }),
          Counter.assert({ name: 'test', value: 5 }),
        ],
        'returns both facts'
      )

      yield* db.transact([Counter.assert({ name: 'test', value: 0 }).retract()])

      assert.deepEqual(
        yield* Counter().query({ from: db }),
        [Counter.assert({ name: 'test', value: 5 })],
        'one fact was retracted'
      )

      yield* db.clear()
    }))

  it('changes propagate across sessions', async () => {
    const Counter = fact({
      name: String,
      value: Number,
    })

    const db = await Artifacts.open(alice)
    await db.transact([Counter.assert({ name: 'test', value: 10 })])

    const session = await Artifacts.open(alice)

    assert.deepEqual(
      await Counter().query({ from: session }),
      [Counter.assert({ name: 'test', value: 10 })],
      'new session picks up where last lefts off'
    )

    await session.transact([
      Counter.assert({ name: 'test', value: 10 }).retract(),
      Counter.assert({ name: 'test', value: 15 }),
    ])

    assert.deepEqual(
      await Counter().query({ from: session }),
      [Counter.assert({ name: 'test', value: 15 })],
      'transacted state'
    )

    assert.deepEqual(
      await Counter().query({ from: db }),
      [Counter.assert({ name: 'test', value: 15 })],
      'change propagated'
    )

    await db.clear()
  })
})
