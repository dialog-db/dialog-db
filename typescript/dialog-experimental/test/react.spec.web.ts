import { Artifacts, Query, fact, Task } from './self.js'
import { useQuery, Provider } from '@dialog-db/experimental/react'
import { createElement } from 'react'
import { act, render, renderHook } from '@testing-library/react'
import { assert } from '@open-wc/testing'

describe('react integration', () => {
  it('test hook', async () => {
    const db = await Artifacts.open({ name: 'test-hook' })
    const Counter = fact({
      name: String,
      value: Number,
    })

    const { result, rerender } = renderHook(() => useQuery(Counter(), db))
    assert.deepEqual(result.current, [], 'returns nothing at first')

    const v1 = Counter.assert({ name: 'test', value: 1 })
    await db.transact(v1)

    rerender()

    assert.deepEqual(result.current, [v1])
  })

  it('test hook with provider', async () => {
    const db = await Artifacts.open({ name: 'test-provider' })
    const Todo = fact({
      title: String,
      done: Boolean,
    })

    function TodoList() {
      const todos = useQuery(Todo())

      return createElement(
        'div',
        null,
        todos.map((todo) =>
          createElement('p', { key: todo.this.toString() }, todo.title)
        )
      )
    }

    function App() {
      return createElement(Provider, { value: db }, [
        createElement(TodoList, { key: 'todo-list' }),
      ])
    }

    const { container } = render(createElement(App))

    assert.deepEqual(
      [...container.querySelectorAll('p')].map((node) => node.textContent),
      [],
      'first we have no children'
    )

    await act(async () => {
      await db.transact([
        ...Todo.assert({ title: 'Buy Milk', done: false }),
        ...Todo.assert({ title: 'Buy Bread', done: false }),
      ])
    })

    assert.deepEqual(
      [...container.querySelectorAll('p')].map((node) => node.textContent),
      ['Buy Milk', 'Buy Bread'],
      'now it has two todo items'
    )
  })
})
