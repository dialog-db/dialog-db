//! The read of the greatest keys of a collection over a stack
//! ([`StackQuery::keys`](super::StackQuery::keys)).
//!
//! A collection is the entries of one domain of attributes under one entity.
//! The name half of the attribute of each entry is its key. The read takes
//! the greatest keys of one collection, below a key or from the end, with one
//! reverse scan of each layer that stops after the count of keys. These tests
//! check what it answers, and that it takes about the count of rows from the
//! layers, whatever the length of the collection.

use anyhow::Result;
use dialog_corpus::position::{self, Bias, Position};
use dialog_corpus::{
    Changes, Entity, Name, Pick, Relation as CorpusRelation, Statement as _, Symbol, Update as _,
    Value,
};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::Keyed;
use dialog_query::attribute::AttributeStatement;
use dialog_storage::provider::storage::VolatileSpace;

use super::{CollectionKeys, Stack};
use crate::helpers::test_repo;
use crate::{Branch, Ephemeral, Placement};

type Operator = Peer<VolatileSpace, Session>;

/// The domain of the collection that these tests read.
const DOMAIN: &str = "todo.list";

/// The entity that holds the collection.
fn list() -> Entity {
    "list:groceries".parse().expect("an entity")
}

/// The scope `name`.
fn scope(name: &str) -> Entity {
    format!("memory:{name}").parse().expect("a scope")
}

/// The domain of the collection, as a symbol.
fn domain() -> Symbol {
    DOMAIN.parse().expect("a domain")
}

/// The keys of a sequence of `count` entries, in the order of the entries:
/// each a position after the one before it, as `dialog/position` derives one
/// for each append.
fn positions(count: usize) -> Result<Vec<String>> {
    let mut keys = Vec::with_capacity(count);
    let mut last: Option<Position> = None;
    for number in 0..count {
        let bias = Bias::derive(format!("item:{number}").as_bytes());
        let next = match &last {
            Some(last) => position::insert(&bias, last..),
            None => position::insert(&bias, ..),
        }
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        keys.push(next.as_str().to_string());
        last = Some(next);
    }
    Ok(keys)
}

/// The entry of `of` under `key` of the collection, whose member is
/// `member`. An entry is one claim among many of its attribute, so two
/// entries can share one key.
fn entry(of: &Entity, key: &str, member: &str) -> Result<AttributeStatement> {
    Ok(AttributeStatement {
        the: CorpusRelation::try_from(format!("{DOMAIN}/{key}"))?.into(),
        of: of.clone(),
        is: Value::Entity(member.parse()?),
        cause: None,
        cardinality: Some(dialog_query::Cardinality::Many),
        pick: None,
    })
}

/// A fact of `of` outside the collection.
fn fact(the: &str, of: &Entity, is: &str) -> Result<AttributeStatement> {
    Ok(AttributeStatement {
        the: the.parse()?,
        of: of.clone(),
        is: Value::String(is.into()),
        cause: None,
        cardinality: Some(dialog_query::Cardinality::Many),
        pick: None,
    })
}

/// Commits the entry of `list()` under each of `keys`, a few thousand in a
/// commit, each with a member of its own.
async fn hold(branch: &Branch, env: &Operator, keys: &[String]) -> Result<()> {
    let of = list();
    for (chunk, keys) in keys.chunks(5_000).enumerate() {
        let mut transaction = branch.transaction();
        for (at, key) in keys.iter().enumerate() {
            transaction = transaction.assert(entry(&of, key, &format!("item:{chunk}-{at}"))?);
        }
        transaction.commit().publish().perform(env).await?;
    }
    Ok(())
}

/// A stack over `branch` alone, which is its bottom.
async fn over(env: &Operator, branch: &Branch) -> Result<Stack> {
    let top = Ephemeral::create().perform(env).await;
    Ok(Stack::open(top.clone())
        .link(&top, branch, scope("shared"))
        .perform(env)
        .await?)
}

/// The greatest keys of the collection over `stack`, below `below`.
async fn read(
    stack: &Stack,
    env: &Operator,
    keyed: Keyed,
    below: Option<&str>,
    count: usize,
) -> Result<CollectionKeys> {
    let below = below.map(Name::try_from).transpose()?;
    Ok(stack
        .query()
        .keys(list(), domain(), keyed, below, count)
        .perform(env)
        .await?)
}

/// `keys`, the greatest first.
fn greatest_first(keys: &[String]) -> Vec<String> {
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    keys.reverse();
    keys
}

/// A read of the 50 greatest keys of a sequence of 1,000 entries answers
/// them, the greatest first. A read below the least of them answers the next
/// 50, and so on to the start of the sequence, and a read below the least key
/// answers none. The facts beside the collection stay out of each answer: the
/// other attributes of the entity, the entries of a dictionary in the same
/// domain, and the same collection of another entity.
#[dialog_common::test]
async fn it_reads_the_greatest_keys_of_a_sequence_page_by_page() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    let keys = positions(1_000)?;
    hold(&shared, &operator, &keys).await?;
    let of = list();
    let other: Entity = "list:chores".parse()?;
    shared
        .transaction()
        .assert(fact("todo.box/name", &of, "below the domain")?)
        .assert(fact("todo.list.zone/name", &of, "above the domain")?)
        .assert(fact("todo.lists/name", &of, "after the domain")?)
        .assert(entry(&of, "note", "item:dictionary")?)
        .assert(entry(&other, &keys[999], "item:other")?)
        .assert(entry(&other, "Zz", "item:other-last")?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let stack = over(&operator, &shared).await?;

    let expected = greatest_first(&keys);
    let first = read(&stack, &operator, Keyed::Sequence, None, 50).await?;
    assert_eq!(
        first.keys,
        expected[..50],
        "the 50 greatest keys, the greatest first"
    );
    assert_eq!(first.rows, 50, "one row for each key");

    let second = read(
        &stack,
        &operator,
        Keyed::Sequence,
        first.keys.last().map(String::as_str),
        50,
    )
    .await?;
    assert_eq!(
        second.keys,
        expected[50..100],
        "the next 50 below the least of them"
    );

    // Page on to the start: each page is the next 50, and together they are
    // the whole sequence.
    let mut paged = first.keys.clone();
    paged.extend(second.keys.clone());
    while let Some(last) = paged.last().cloned() {
        let page = read(&stack, &operator, Keyed::Sequence, Some(&last), 50).await?;
        if page.keys.is_empty() {
            break;
        }
        assert!(page.keys.len() <= 50);
        paged.extend(page.keys);
    }
    assert_eq!(
        paged, expected,
        "the pages are the whole sequence, in order"
    );

    let least = expected.last().expect("a key");
    let none = read(&stack, &operator, Keyed::Sequence, Some(least), 50).await?;
    assert!(none.keys.is_empty(), "no key is below the least key");

    // The dictionary of the same domain is the other half of it.
    let dictionary = read(&stack, &operator, Keyed::Dictionary, None, 50).await?;
    assert_eq!(dictionary.keys, vec!["note".to_string()]);
    Ok(())
}

/// A key that two entries share is in an answer once, and the read takes a
/// row for each of its entries.
#[dialog_common::test]
async fn it_answers_a_key_that_two_entries_share_once() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    let keys = positions(100)?;
    hold(&shared, &operator, &keys).await?;
    let expected = greatest_first(&keys);
    let twice = &expected[3];
    shared
        .transaction()
        .assert(entry(&list(), twice, "item:moved")?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let stack = over(&operator, &shared).await?;

    let answer = read(&stack, &operator, Keyed::Sequence, None, 10).await?;
    assert_eq!(
        answer.keys,
        expected[..10],
        "the shared key is in the answer once"
    );
    assert_eq!(answer.rows, 11, "the shared key is two rows");
    Ok(())
}

/// A read of 50 keys takes 50 rows from the layers, for a collection of
/// 100,000 entries as for one of 1,000: its cost follows the count, and not
/// the length of the collection.
#[dialog_common::test]
async fn it_reads_about_count_rows_of_a_collection_of_100_000_entries() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let small = repo.branch("small").open().perform(&operator).await?;
    let large = repo.branch("large").open().perform(&operator).await?;
    let keys = positions(100_000)?;
    hold(&small, &operator, &keys[..1_000]).await?;
    hold(&large, &operator, &keys).await?;

    for (branch, keys) in [(&small, &keys[..1_000]), (&large, &keys[..])] {
        let stack = over(&operator, branch).await?;
        let expected = greatest_first(keys);
        let end = read(&stack, &operator, Keyed::Sequence, None, 50).await?;
        assert_eq!(end.keys, expected[..50]);
        assert_eq!(
            end.rows,
            50,
            "a read of 50 keys of {} entries takes 50 rows",
            keys.len()
        );
        let middle = &expected[keys.len() / 2];
        let below = read(&stack, &operator, Keyed::Sequence, Some(middle), 50).await?;
        let at = keys.len() / 2 + 1;
        assert_eq!(below.keys, expected[at..at + 50]);
        assert_eq!(below.rows, 50);
    }
    Ok(())
}

/// Over a stack of two layers, the answer holds the keys of both layers, in
/// the order of the keys, and a key that both hold once. A key whose
/// attribute a placement keeps from the subject is not in it, though the
/// subject holds an entry under it; the layer of its scope gives it, when that
/// layer holds it.
#[dialog_common::test]
async fn it_reads_the_keys_of_two_layers_by_their_placements() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let shared = test_repo(&operator, &peer)
        .await
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    let device = test_repo(&operator, &peer)
        .await
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    let keys = positions(40)?;
    // The subject holds the entries of even number, the other layer those
    // of odd number, and both hold the last.
    let even: Vec<String> = keys.iter().step_by(2).cloned().collect();
    let mut odd: Vec<String> = keys.iter().skip(1).step_by(2).cloned().collect();
    hold(&shared, &operator, &even).await?;
    odd.push(keys[38].clone());
    hold(&device, &operator, &odd).await?;

    let top = Ephemeral::create().perform(&operator).await;
    let stack = Stack::open(top.clone())
        .link(&top, &shared, scope("shared"))
        .link(&top, &device, scope("device"))
        .perform(&operator)
        .await?;

    let expected = greatest_first(&keys);
    let both = read(&stack, &operator, Keyed::Sequence, None, 20).await?;
    assert_eq!(
        both.keys,
        expected[..20],
        "the keys of both layers, in order"
    );
    let rest = read(
        &stack,
        &operator,
        Keyed::Sequence,
        both.keys.last().map(String::as_str),
        100,
    )
    .await?;
    assert_eq!(
        rest.keys,
        expected[20..],
        "and below them, the rest of both"
    );

    // The session store of the bottom places two attributes of the
    // collection on the other layer: the entry of the subject under each
    // stays out of every read. The other layer holds one of them.
    let kept = &keys[10];
    let given = &keys[38];
    for key in [kept, given] {
        shared.overlay().assert(Placement::new(
            format!("{DOMAIN}/{key}").parse()?,
            scope("device"),
        ))?;
    }
    let placed = read(&stack, &operator, Keyed::Sequence, None, 100).await?;
    let mut expected: Vec<String> = keys.iter().filter(|key| *key != kept).cloned().collect();
    expected = greatest_first(&expected);
    assert_eq!(
        placed.keys, expected,
        "the placed key that only the subject holds is not in the answer"
    );
    assert!(
        placed.keys.contains(given),
        "the layer of its scope gives the other"
    );
    assert!(
        stack
            .divergences()
            .iter()
            .any(|divergence| divergence.attribute == format!("{DOMAIN}/{kept}")),
        "the read reports the entry it kept out"
    );
    Ok(())
}

/// The read takes what a query of the stack reads beside the trees: the
/// session store of the bottom, an ephemeral layer, and the changes of the
/// query, whose retraction hides an entry of a layer. Its answer is the keys
/// of all of them, the greatest first.
#[dialog_common::test]
async fn it_reads_the_keys_of_each_source_of_a_query() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    let keys = positions(30)?;
    hold(&shared, &operator, &keys[..20]).await?;
    let state = Ephemeral::create().perform(&operator).await;
    let top = Ephemeral::create().perform(&operator).await;
    let stack = Stack::open(top.clone())
        .link(&top, &shared, scope("shared"))
        .link(&top, &state, scope("state"))
        .perform(&operator)
        .await?;
    let of = list();

    // The session store of the bottom, the ephemeral layer, and the query.
    shared
        .overlay()
        .assert(entry(&of, &keys[20], "item:session")?)?;
    let mut held = Changes::new();
    for key in &keys[21..25] {
        entry(&of, key, "item:state")?.assert(&mut held);
    }
    state.apply(held)?;
    let mut with = Changes::new();
    for key in &keys[25..30] {
        entry(&of, key, "item:with")?.assert(&mut with);
    }
    // The entry of the tree under `keys[5]` is item:0-5, as `hold` names it.
    let hidden = &keys[5];
    with.dissociate(
        format!("{DOMAIN}/{hidden}").parse()?,
        of.clone(),
        Value::Entity("item:0-5".parse()?),
    );
    // A dictionary entry of the query stays out of a read of the sequence.
    with.associate(
        format!("{DOMAIN}/note").parse()?,
        of.clone(),
        Value::Entity("item:note".parse()?),
        Pick::All,
    );

    let answer = stack
        .query()
        .with(with)
        .keys(of, domain(), Keyed::Sequence, None, 100)
        .perform(&operator)
        .await?;
    let visible: Vec<String> = keys.iter().filter(|key| *key != hidden).cloned().collect();
    assert_eq!(answer.keys, greatest_first(&visible));
    Ok(())
}

/// A read of a transaction takes its staged writes beside the rows of the
/// tree and of the session store of the bottom, all in the order of the keys.
/// The tree, the session store, and the staged writes each hold every third
/// key, so the three sources interleave key by key. One key is in all three,
/// and one is in the tree and the staged writes. The transaction retracts the
/// entry of the tree under one key and the entry of the session store under
/// another, so neither key is in the answer. A read of the whole collection,
/// and reads page by page, answer the same keys, the greatest first, each
/// once.
#[dialog_common::test]
async fn it_reads_the_keys_of_the_staged_writes_of_a_transaction_in_order() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    let keys = positions(30)?;
    let of = list();

    // The tree holds the keys 0, 3, 6, and so on, under the members that
    // `hold` names `item:0-0`, `item:0-1`, and so on.
    let tree: Vec<String> = keys.iter().step_by(3).cloned().collect();
    hold(&shared, &operator, &tree).await?;
    // The session store of the bottom holds the keys 1, 4, 7, and so on,
    // and the key 9 that the tree holds too.
    for key in keys.iter().skip(1).step_by(3).chain([&keys[9]]) {
        shared
            .overlay()
            .assert(entry(&of, key, &format!("item:session-{key}"))?)?;
    }
    let stack = over(&operator, &shared).await?;

    // The staged writes hold the keys 2, 5, 8, and so on, the key 9 of the
    // tree and the session store, and the key 15 of the tree.
    let mut transaction = stack.transaction();
    for key in keys.iter().skip(2).step_by(3).chain([&keys[9], &keys[15]]) {
        transaction = transaction.assert(entry(&of, key, &format!("item:staged-{key}"))?);
    }
    // The tree holds the key 12 as its fifth entry, and the session store
    // holds the key 13. Each has one entry, so each leaves the answer.
    let retracted = [&keys[12], &keys[13]];
    transaction = transaction
        .retract(entry(&of, retracted[0], "item:0-4")?)
        .retract(entry(
            &of,
            retracted[1],
            &format!("item:session-{}", keys[13]),
        )?);

    let visible: Vec<String> = keys
        .iter()
        .filter(|key| !retracted.contains(key))
        .cloned()
        .collect();
    let expected = greatest_first(&visible);
    let query = transaction.query();
    let read = |below: Option<String>, count: usize| {
        let query = query.clone();
        let of = of.clone();
        let operator = &operator;
        async move {
            let below = below.as_deref().map(Name::try_from).transpose()?;
            anyhow::Ok(
                query
                    .keys(of, domain(), Keyed::Sequence, below, count)
                    .perform(operator)
                    .await?,
            )
        }
    };

    let whole = read(None, 100).await?;
    assert_eq!(
        whole.keys, expected,
        "the keys of the tree, the session store, and the staged writes, in order"
    );
    assert_eq!(
        whole.rows,
        expected.len() + 3,
        "the key 9 is three rows, and the key 15 is two"
    );

    // Pages of 4 keys cross from one source to the next at each key.
    let mut paged: Vec<String> = Vec::new();
    loop {
        let page = read(paged.last().cloned(), 4).await?;
        if page.keys.is_empty() {
            break;
        }
        paged.extend(page.keys);
    }
    assert_eq!(
        paged, expected,
        "the pages are the whole collection, in order"
    );
    Ok(())
}

/// A read of a dictionary takes its symbols, the greatest first, below a
/// symbol too.
#[dialog_common::test]
async fn it_reads_the_greatest_keys_of_a_dictionary() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    let names: Vec<String> = (0..30).map(|number| format!("n{number:03}")).collect();
    hold(&shared, &operator, &names).await?;
    hold(&shared, &operator, &positions(5)?).await?;
    let stack = over(&operator, &shared).await?;

    let answer = read(&stack, &operator, Keyed::Dictionary, Some("n020"), 5).await?;
    let expected: Vec<String> = (15..20)
        .rev()
        .map(|number| format!("n{number:03}"))
        .collect();
    assert_eq!(answer.keys, expected);
    Ok(())
}

/// A count of zero answers nothing, and a bound of the other kind of key is
/// refused: a symbol sorts above each position, so it would bound nothing.
#[dialog_common::test]
async fn it_refuses_a_bound_of_the_other_kind_of_key() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let shared = repo.branch("main").open().perform(&operator).await?;
    hold(&shared, &operator, &positions(5)?).await?;
    let stack = over(&operator, &shared).await?;

    let none = read(&stack, &operator, Keyed::Sequence, None, 0).await?;
    assert!(none.keys.is_empty());
    assert_eq!(none.rows, 0);
    assert!(
        read(&stack, &operator, Keyed::Sequence, Some("note"), 5)
            .await
            .is_err(),
        "a symbol bounds no sequence"
    );
    let position = positions(1)?.remove(0);
    assert!(
        read(&stack, &operator, Keyed::Dictionary, Some(&position), 5)
            .await
            .is_err(),
        "a position bounds no dictionary"
    );
    Ok(())
}

/// The read is a future that a host can send to another thread.
#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn it_reads_on_any_thread(stack: &Stack, env: &Operator) {
    // bare-send-ok: native only; a host spawns the read on a thread pool
    fn send<T: Send>(_: &T) {}
    let read = stack
        .query()
        .keys(list(), domain(), Keyed::Sequence, None, 1)
        .perform(env);
    send(&read);
}
