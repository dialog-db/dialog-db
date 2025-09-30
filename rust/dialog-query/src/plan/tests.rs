use super::*;
use crate::artifact::Artifacts;
use crate::{Session, Term, Value};
use dialog_storage::MemoryStorageBackend;

#[tokio::test]
async fn test_fresh_context_has_empty_scope() {
    let storage = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage).await.unwrap();
    let session = Session::open(artifacts);

    let context = fresh(session);

    // Fresh context should have empty scope
    assert_eq!(context.scope.size(), 0);
}

#[tokio::test]
async fn test_context_with_scope() {
    let storage = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage).await.unwrap();
    let session = Session::open(artifacts);

    let original_context = fresh(session.clone());

    // Create a scope with a variable
    let mut scope = VariableScope::new();
    scope.add(&Term::<Value>::var("x"));
    scope.add(&Term::<Value>::var("y"));

    let scoped_context = original_context.with_scope(scope.clone());

    // New context should have the provided scope
    assert_eq!(scoped_context.scope.size(), 2);
    assert!(scoped_context.scope.contains(&Term::<Value>::var("x")));
    assert!(scoped_context.scope.contains(&Term::<Value>::var("y")));
}

#[tokio::test]
async fn test_context_single_with_scope() {
    let storage = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage).await.unwrap();
    let session = Session::open(artifacts);

    let selection = once(async move { Ok(Match::new()) });
    let mut scope = VariableScope::new();
    scope.add(&Term::<Value>::var("z"));

    let context = EvaluationContext::single(session, selection, scope.clone());

    // Context should have the provided scope
    assert_eq!(context.scope.size(), 1);
    assert!(context.scope.contains(&Term::<Value>::var("z")));
}

#[tokio::test]
async fn test_scope_preserved_through_with_scope() {
    let storage = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage).await.unwrap();
    let session = Session::open(artifacts);

    let context = fresh(session.clone());

    // Create first scope
    let mut scope1 = VariableScope::new();
    scope1.add(&Term::<Value>::var("a"));

    let context1 = context.with_scope(scope1);

    // Create second scope
    let mut scope2 = VariableScope::new();
    scope2.add(&Term::<Value>::var("a"));
    scope2.add(&Term::<Value>::var("b"));

    let context2 = context1.with_scope(scope2);

    // Second context should have new scope
    assert_eq!(context2.scope.size(), 2);
    assert!(context2.scope.contains(&Term::<Value>::var("a")));
    assert!(context2.scope.contains(&Term::<Value>::var("b")));
}