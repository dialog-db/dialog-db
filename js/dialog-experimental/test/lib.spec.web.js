import { Artifacts, Query, fact, Task } from './self.js'

/** @type {import('entail').Suite} */
export const testLib = {
  'test imports': async (assert) => {
    assert.equal(typeof Artifacts, 'object')
    assert.equal(typeof Query, 'object')
  },
  'test basics': (assert) =>
    Task.spawn(function* () {
      const Counter = fact({
        name: String,
        value: Number,
      })

      const db = yield* Artifacts.open({ name: 'test' })

      yield* db.transact([...Counter.assert({ name: 'test', value: 0 })])

      const results = yield* Counter().query({ from: db })

      assert.deepEqual(results, [Counter.assert({ name: 'test', value: 0 })])

      yield* db.transact([...Counter.assert({ name: 'test', value: 5 })])

      assert.deepEqual(
        yield* Counter().query({ from: db }),
        [
          Counter.assert({ name: 'test', value: 0 }),
          Counter.assert({ name: 'test', value: 5 }),
        ],
        'returns both facts'
      )

      yield* db.transact([
        ...Counter.assert({ name: 'test', value: 0 }).retract(),
      ])

      assert.deepEqual(
        yield* Counter().query({ from: db }),
        [Counter.assert({ name: 'test', value: 5 })],
        'one fact was retracted'
      )
    }),
}
