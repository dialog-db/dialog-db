import { Artifacts, Query, fact, Task, API } from './self.js'
import {
  useQuery,
  Provider,
  useSession,
  useTransaction,
} from '@dialog-db/experimental/react'
import * as React from 'react'

import {
  act,
  render,
  screen,
  fireEvent,
  renderHook,
} from '@testing-library/react'
import { assert } from '@open-wc/testing'
import { bob } from './constants.js'

describe('react integration', () => {
  it('test hook', async () => {
    const db = Artifacts.open(bob)
    try {
      const Counter = fact({
        name: String,
        value: Number,
      })

      const { result, rerender } = renderHook(() => useQuery(Counter(), db))
      assert.deepEqual(result.current, [], 'returns nothing at first')

      const v1 = Counter.assert({ name: 'test', value: 1 })
      await db.transact([v1])

      await Task.perform(Task.sleep(100))

      rerender()

      assert.deepEqual(result.current, [v1])
    } finally {
      await db.clear()
    }
  })

  it('test useQuery with a provider', async () => {
    const session = Artifacts.open(bob)
    try {
      const Todo = fact({
        title: String,
        done: Boolean,
      })

      const TodoItem = (todo: {
        title: string
        done: boolean
        this: object
      }) => <p key={todo.this.toString()}>{todo.title}</p>

      function TodoList() {
        const todos = useQuery(Todo())

        return <div>{todos.map((todo) => TodoItem(todo))}</div>
      }
      const App = () => (
        <Provider value={bob}>
          <TodoList key="todo-list" />
        </Provider>
      )

      const { container } = render(<App />)
      assert.deepEqual(
        [...container.querySelectorAll('p')].map((node) => node.textContent),
        [],
        'first we have no children'
      )
      await act(async () => {
        await session.transact([
          Todo.assert({ title: 'Buy Milk', done: false }),
          Todo.assert({ title: 'Buy Bread', done: false }),
        ])

        // Wait for broadcast channel to propagate updates
        await new Promise((resolve) => setTimeout(resolve, 200))
      })

      assert.deepEqual(
        [...container.querySelectorAll('p')].map((node) => node.textContent),
        ['Buy Milk', 'Buy Bread'],
        'now it has two todo items'
      )
    } finally {
      await session.clear()
    }
  })

  it('test useTransaction', async () => {
    const session = Artifacts.open(bob)
    try {
      const Counter = fact({
        title: String,
        count: Number,
      })

      const counter = Counter.assert({ title: 'demo', count: 0 })
      await session.transact([counter])
      const id = counter.this

      assert.deepEqual(
        await Counter.match({ this: id }).query({ from: session }),
        [counter]
      )

      let clicked = 0
      function View() {
        const transact = useTransaction()
        const [counter] = useQuery(Counter.match({ this: id }))

        return counter ?
            <div>
              <button
                title="increment"
                onClick={() => {
                  clicked++
                  transact([
                    counter.retract(),
                    Counter.assert({
                      this: counter.this,
                      title: 'new title',
                      count: counter.count + 1,
                    }),
                  ])
                }}
              >
                +
              </button>
              <code>{counter.count}</code>
            </div>
          : <div></div>
      }

      const App = () => (
        <Provider value={bob}>
          <View />
        </Provider>
      )

      const { container } = render(<App />)

      // wait for react to re-render after query is run
      await Task.perform(Task.sleep(100))

      assert.deepEqual(
        [...container.querySelectorAll('code')].map((node) => node.textContent),
        ['0'],
        'counter is set to 0'
      )

      fireEvent.click(screen.getByTitle('increment'))

      assert.ok(clicked > 0)

      // wait for react to re-render after transaction
      await Task.perform(Task.sleep(100))

      assert.deepEqual(
        [...container.querySelectorAll('code')].map((node) => node.textContent),
        ['1'],
        'counter incremented'
      )
    } finally {
      await session.clear()
    }
  })
})
