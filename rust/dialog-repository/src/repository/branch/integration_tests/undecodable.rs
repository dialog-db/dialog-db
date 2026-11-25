//! A claim whose stored value this dialog cannot read, arriving by a
//! pull.
//!
//! A typed write cannot produce such a claim, and a pull takes claims
//! from writers this replica does not control, such as another
//! implementation, so a replica can hold one. Every read and write treats
//! it as if it did not exist (see `undecodable_tests`): the cell answers
//! its readable claims, and a cardinality-one write to the cell evicts
//! it, on every replica that pulls the write.

use super::super::undecodable_tests::{claim, next_version, read_one, stored, undecodable};
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

/// A replica that pulled a claim no reader can read, newer than its
/// cell's readable one, answers the readable one, as the replica it came
/// from does, and neither read fails.
#[dialog_common::test]
async fn a_pulled_undecodable_claim_is_passed_over(s3: S3Address) -> Result<()> {
    let devices = pulled(&s3, "undecodable-read").await?;
    assert_eq!(
        read_one(&devices.bob, "item/size", &devices.env).await?,
        vec![Value::String("first".into())]
    );
    assert_eq!(
        read_one(&devices.alice, "item/size", &devices.operator).await?,
        vec![Value::String("first".into())]
    );
    Ok(())
}

/// A cardinality-one write to a cell holding a pulled claim no reader can
/// read lands and evicts it: the writer reads the written value and its
/// head stores that claim alone, and so does the replica the claim came
/// from once it pulls the write.
#[dialog_common::test]
async fn a_cardinality_one_write_evicts_a_pulled_undecodable_claim(s3: S3Address) -> Result<()> {
    let devices = pulled(&s3, "undecodable-write").await?;
    assert_eq!(
        stored(&devices.alice, "item/size", &devices.operator).await?,
        2
    );
    devices
        .bob
        .commit(stream::iter(vec![size("fine", Cardinality::One)?]))
        .perform(&devices.env)
        .await?;
    assert_eq!(
        read_one(&devices.bob, "item/size", &devices.env).await?,
        vec![Value::String("fine".into())]
    );
    assert_eq!(
        stored(&devices.bob, "item/size", devices.env.inner()).await?,
        1,
        "the write evicts the undecodable claim"
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
        vec![Value::String("fine".into())]
    );
    assert_eq!(
        stored(&devices.alice, "item/size", &devices.operator).await?,
        1,
        "the eviction reaches the replica the undecodable claim came from"
    );
    Ok(())
}
