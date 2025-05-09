import type { Predicate, FactSchema } from '@dialog-db/query'
import { useEffect, useState, useContext, createContext } from 'react'
import { type Source } from './artifacts.js'

const QueryContext = createContext<Source | null>(null)

export const { Provider } = QueryContext

/**
 * @param predicate - Predicate hook will react to.
 * @param source - dialog-db instance, can be passed as an argument or
 * provided through a provider.
 *
 * @example
 * ```ts
 * const Todo = fact({
 *   title: String,
 *   done: Boolean,
 * })
 *
 * function TodoList() {
 *   const [todos] = useQuery(Todo(), db)
 *
 *   return (<div>
 *    <h2>Your todos are:</h2>
 *    {todos.map(todo => (<p key={todo.this}>{todo.title}</p>))}
 *   </div>)
 * }
 * ```
 */
export const useQuery = <Fact>(
  predicate: Predicate<Fact, string, FactSchema>,
  source?: Source
) => {
  const [facts, setFacts] = useState([] as Fact[])
  const artifacts = source ?? useContext(QueryContext)

  useEffect(() => {
    if (artifacts) {
      artifacts.subscribe(predicate, setFacts).cancel
    }
  }, [artifacts])
  return facts
}
