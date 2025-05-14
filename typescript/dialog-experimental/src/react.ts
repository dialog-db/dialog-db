import type { Predicate, FactSchema } from '@dialog-db/query'
import { useEffect, useState, useContext, createContext, useMemo } from 'react'
import type { Session, Changes, DID, Revision } from './artifacts.js'
import { open } from './artifacts.js'

const DialogContext = createContext<DID | null>(null)

/**
 * Provider that can be used to bind a dialog db instance.
 */
export const { Provider } = DialogContext

/**
 * React hook that can be called at the top level of your component to obtain
 * dialog session that was set using exported provider.
 */
export const useSession = () => {
  const did = useContext(DialogContext)
  return useMemo(() => (did ? open(did) : null), [did])
}

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
  source?: Session
) => {
  const [selection, resetSelection] = useState([] as Fact[])
  const session = source ?? useSession()

  useEffect(() => {
    if (session) {
      session.subscribe(predicate, resetSelection).cancel
    }
  }, [session])
  return selection
}

/**
 * React hook that can be used from the react component in order to obtain
 * {@link transact} function pre-bound to the dialog session linked from the
 * {@link Provider}.
 */
export const useTransaction = () => {
  const session = useSession()

  return (changes: Changes) => transact(changes, session!)
}

/**
 * Transacts all of the assertions and retractions atomically in the provided
 * dialog session.
 */
export const transact = (changes: Changes, session: Session) =>
  session.transact(changes)
