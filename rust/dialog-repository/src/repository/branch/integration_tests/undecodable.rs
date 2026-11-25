//! A claim whose stored value no reader can decode, arriving by a pull.
//!
//! A typed write cannot produce such a claim, and nothing checks a
//! claim's value when a replica pulls it, so a replica takes it from
//! another writer. Once it holds the claim, its cell must stay readable
//! and writable: a cardinality-one read answers what it would if the bad
//! claim could not be read at all (see `undecodable_tests`), and a
//! cardinality-one write to the cell replaces the bad claim, as it
//! replaces any claim it reads, on every replica that pulls the write.

use super::super::undecodable_tests::{claim, next_version, read_one, undecodable};
use super::*;
use dialog_corpus::Cardinality;

/// `item/size` of the item as `size`, asserted under `cardinality`.
fn size(size: &str, cardinality: Cardinality) -> Result<Instruction> {
    Ok(Instruction::Assert(
        claim("item/size", Value::String(size.into()))?,
        cardinality,
    ))
}

/// Alice writes the item's size, then a claim of its size that is not
/// UTF-8, the newer of the two, and publishes; Bob pulls both.
async fn pulled(s3: &S3Address, name: &str) -> Result<Followed> {
    let devices = followed(s3, name).await?;
    devices
        .alice
        .commit(stream::iter(vec![size("first", Cardinality::One)?]))
        .perform(&devices.operator)
        .await?;
    devices
        .alice
        .commit(stream::iter(Vec::<Instruction>::new()))
        .with_entries(undecodable(
            &claim("item/size", Value::String("QZ".into()))?,
            b"QZ",
            b"\xff\xfe",
            &next_version(&devices.alice)?,
        )?)
        .perform(&devices.operator)
        .await?;
    assert!(
        devices
            .alice
            .push()
            .perform(&devices.operator)
            .await?
            .is_some(),
        "alice publishes"
    );
    assert!(
        devices
            .bob
            .pull(crate::Download::MinimalRanges)
            .perform(&devices.env)
            .await?
            .is_some(),
        "bob takes alice's head"
    );
    Ok(devices)
}

/// A replica that pulled an undecodable claim newer than its cell's
/// readable one answers nothing for the cell, not the readable claim the
/// bad one stands over, and the read does not fail.
#[dialog_common::test]
async fn a_pulled_undecodable_claim_leaves_its_cell_readable(s3: S3Address) -> Result<()> {
    let devices = pulled(&s3, "undecodable-read").await?;
    assert_eq!(
        read_one(&devices.bob, "item/size", &devices.env).await?,
        Vec::<Value>::new()
    );
    assert_eq!(
        read_one(&devices.alice, "item/size", &devices.operator).await?,
        Vec::<Value>::new()
    );
    Ok(())
}

/// A cardinality-one write to a cell holding an undecodable claim lands,
/// and replaces the claim: the writer reads the written value, and so
/// does the replica the claim came from once it pulls the write.
#[dialog_common::test]
async fn a_cardinality_one_write_replaces_a_pulled_undecodable_claim(s3: S3Address) -> Result<()> {
    let devices = pulled(&s3, "undecodable-write").await?;
    devices
        .bob
        .commit(stream::iter(vec![size("fine", Cardinality::One)?]))
        .perform(&devices.env)
        .await?;
    assert_eq!(
        read_one(&devices.bob, "item/size", &devices.env).await?,
        vec![Value::String("fine".into())]
    );

    assert!(
        devices.bob.push().perform(&devices.env).await?.is_some(),
        "bob publishes"
    );
    devices
        .alice
        .pull(crate::Download::MinimalRanges)
        .perform(&devices.operator)
        .await?;
    assert_eq!(
        read_one(&devices.alice, "item/size", &devices.operator).await?,
        vec![Value::String("fine".into())],
        "the write replaces the undecodable claim on the replica it came from"
    );
    Ok(())
}
