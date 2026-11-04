//! Where a subject's blocks hydrate from: the routes registered on an
//! environment for it.
//!
//! A block a replica holds by reference is not in its local archive, and
//! the archive cannot say where it is. What can is whatever tracks a
//! remote for the subject (the repository layer's open branches), which
//! registers a [`Routing`] here. A read that misses locally is answered by
//! the environment from the routes registered for its subject; a read the
//! local archive answers asks nothing here.
//!
//! The registry lives in this crate, beneath both the repository layer
//! that registers routes and the operator layer that hydrates through
//! them, so the two name one type, as they do [`Hydrate`](crate::Hydrate).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use dialog_capability::Did;
use dialog_common::{ConditionalSend, ConditionalSync, Holds, held_key};

use crate::NetworkAddress;

/// One place a subject's blocks may hydrate from.
#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    /// The subject's repository at a site.
    Remote {
        /// The site.
        address: NetworkAddress,
        /// The repository's DID at that site.
        subject: Did,
    },
    /// A remote the subject's content may live at, which could not be
    /// reached when the route was worked out. A miss that finds nothing
    /// elsewhere fails naming it, rather than reading as absent.
    Unavailable {
        /// How the remote is named.
        remote: String,
        /// Why it could not be reached.
        reason: String,
    },
}

/// Something that knows where a subject's blocks live: what a registry
/// asks, on a local miss, for its subject's routes.
pub trait Routing: ConditionalSend + ConditionalSync {
    /// The subject whose blocks it routes.
    fn subject(&self) -> &Did;
    /// Whether it still routes: a registration whose owner is gone is
    /// passed over and dropped.
    fn is_open(&self) -> bool;
    /// Its routes, in the order to try them.
    fn routes(&self) -> Vec<Route>;
}

#[derive(Default)]
struct Registry {
    /// The registrations, in the order they were first made, by key.
    routing: Vec<(String, Arc<dyn Routing>)>,
    /// Routes lent for the length of one operation, newest last.
    lent: Vec<(u64, Did, Route)>,
}

/// The routes an environment hydrates its subjects' blocks from.
///
/// Held by the environment (see [`of`](Self::of)), so everything that
/// reads through it hydrates the same way. Clones share the registry.
#[derive(Clone, Default)]
pub struct Routes {
    inner: Arc<Mutex<Registry>>,
}

impl fmt::Debug for Routes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = self.lock();
        f.debug_struct("Routes")
            .field("routing", &registry.routing.len())
            .field("lent", &registry.lent.len())
            .finish()
    }
}

/// The key an environment holds its routes under.
const HELD: &str = "dialog.routes";

/// Counts the routes lent, so each loan removes its own.
static LOANS: AtomicU64 = AtomicU64::new(0);

impl Routes {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// The routes `env` holds, an empty registry if it holds none yet. An
    /// environment that holds nothing (a bare store) gives a registry of
    /// its own to every caller, so nothing registered on it is ever found:
    /// it hydrates from nowhere.
    pub fn of<Env: Holds + ?Sized>(env: &Env) -> Self {
        env.held_or(&held_key::<Self>(HELD), &|| Arc::new(Self::new()))
            .downcast_ref::<Self>()
            .cloned()
            .unwrap_or_default()
    }

    /// Have `env` hold these routes, in place of any it held.
    pub fn hold<Env: Holds + ?Sized>(&self, env: &Env) {
        env.hold(held_key::<Self>(HELD), Arc::new(self.clone()));
    }

    fn lock(&self) -> MutexGuard<'_, Registry> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Register `routing` under `key`: a registration under a key already
    /// registered replaces it in its place.
    pub fn register(&self, key: String, routing: Arc<dyn Routing>) {
        let mut registry = self.lock();
        registry.routing.retain(|(_, routing)| routing.is_open());
        match registry.routing.iter_mut().find(|(known, _)| *known == key) {
            Some(entry) => entry.1 = routing,
            None => registry.routing.push((key, routing)),
        }
    }

    /// Have `subject`'s blocks hydrate from `route` too, ahead of what is
    /// registered, until the returned loan is dropped: for an operation
    /// that reads a remote's content before anything tracks that remote.
    pub fn lend(&self, subject: &Did, route: Option<Route>) -> Loan {
        let id = LOANS.fetch_add(1, Ordering::Relaxed);
        if let Some(route) = route {
            self.lock().lent.push((id, subject.clone(), route));
        }
        Loan {
            routes: self.clone(),
            id,
        }
    }

    /// Where a local miss of `subject`'s blocks hydrates from, in the order
    /// to try them: the routes lent for it, newest first, then the
    /// registered ones in the order they were registered. Empty when
    /// nothing is registered for it.
    pub fn to(&self, subject: &Did) -> Vec<Route> {
        let (mut routes, routing) = {
            let registry = self.lock();
            let lent: Vec<Route> = registry
                .lent
                .iter()
                .rev()
                .filter(|(_, lent, _)| lent == subject)
                .map(|(_, _, route)| route.clone())
                .collect();
            let routing: Vec<Arc<dyn Routing>> = registry
                .routing
                .iter()
                .filter(|(_, routing)| routing.subject() == subject && routing.is_open())
                .map(|(_, routing)| routing.clone())
                .collect();
            (lent, routing)
        };
        for routing in routing {
            for route in routing.routes() {
                if !routes.contains(&route) {
                    routes.push(route);
                }
            }
        }
        routes
    }

    /// Whether a local miss of `subject`'s blocks has a remote to hydrate
    /// from. Without one every block a read of it finds is local already,
    /// and warming ahead of demand has nothing to do.
    pub fn fetches(&self, subject: &Did) -> bool {
        self.to(subject)
            .iter()
            .any(|route| matches!(route, Route::Remote { .. }))
    }
}

/// A route lent to a registry (see [`Routes::lend`]), taken back when this
/// is dropped.
pub struct Loan {
    routes: Routes,
    id: u64,
}

impl Drop for Loan {
    fn drop(&mut self) {
        self.routes.lock().lent.retain(|(id, _, _)| *id != self.id);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_capability::did;

    struct Fixed(Did, Vec<Route>);

    impl Routing for Fixed {
        fn subject(&self) -> &Did {
            &self.0
        }
        fn is_open(&self) -> bool {
            true
        }
        fn routes(&self) -> Vec<Route> {
            self.1.clone()
        }
    }

    fn unavailable(name: &str) -> Route {
        Route::Unavailable {
            remote: name.into(),
            reason: "down".into(),
        }
    }

    /// Lent routes come first, newest first, then registrations in order,
    /// each route once; a loan dropped takes its route with it.
    #[dialog_common::test]
    fn it_orders_lent_routes_before_registered_ones() {
        let subject = did!("key:zRoutes");
        let routes = Routes::new();
        routes.register(
            "a".into(),
            Arc::new(Fixed(
                subject.clone(),
                vec![unavailable("a"), unavailable("b")],
            )),
        );
        routes.register(
            "b".into(),
            Arc::new(Fixed(
                subject.clone(),
                vec![unavailable("b"), unavailable("c")],
            )),
        );
        let loan = routes.lend(&subject, Some(unavailable("lent")));
        assert_eq!(
            routes.to(&subject),
            vec![
                unavailable("lent"),
                unavailable("a"),
                unavailable("b"),
                unavailable("c")
            ]
        );
        assert!(routes.to(&did!("key:zOther")).is_empty());
        drop(loan);
        assert_eq!(routes.to(&subject).first(), Some(&unavailable("a")));
        assert!(
            !routes.fetches(&subject),
            "an unavailable route fetches nothing"
        );
    }
}
