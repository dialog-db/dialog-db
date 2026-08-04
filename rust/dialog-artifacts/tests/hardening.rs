//! Equivalence pins for the read-path optimizations: each test holds an
//! optimized construction to the semantics of the thing it replaced,
//! through the public API. These are the tests that would have caught the
//! silent-divergence failure modes of this campaign's rewrites.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStoreMutExt as _, Artifacts, Attribute, Cause, Entity,
    Instruction, Uri, Value, default_sort_key,
};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};
use futures_util::TryStreamExt as _;

fn entity(n: u32) -> Entity {
    Entity::from_str(&format!("entity:hardening-{n:04}")).expect("valid entity")
}

/// A value population straddling the encoding decisions: inline strings,
/// strings large enough to spill under the default manifest, numerics,
/// bytes, booleans, and entity references.
fn probe_values() -> Vec<Value> {
    vec![
        Value::String("inline".into()),
        Value::String("x".repeat(4096)),
        Value::UnsignedInt(7),
        Value::SignedInt(-7),
        Value::Float(1.5),
        Value::Boolean(false),
        Value::Bytes(vec![9; 40]),
        Value::Entity(entity(999)),
    ]
}

/// A scanned row's sort key — derived straight from its stored key bytes —
/// must equal the sort key computed from the materialized artifact's
/// fields. The query layer's k-way merge orders and dedups by the stored
/// derivation while the `Changes` overlay uses the field derivation; if
/// the two ever diverge for any value encoding, merged query results
/// interleave wrongly.
#[tokio::test]
async fn it_derives_sort_keys_identical_to_the_field_path() -> Result<()> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend).await?;

    let mut instructions = Vec::new();
    for (at, value) in probe_values().into_iter().enumerate() {
        instructions.push(Instruction::Assert(Artifact {
            the: Attribute::from_str("hardening/value")?,
            of: entity(at as u32),
            is: value,
            cause: if at % 2 == 0 {
                Some(Cause([at as u8 + 1; 32]))
            } else {
                None
            },
        }));
    }
    store.commit(instructions).await?;

    let rows: Vec<_> = store
        .select(ArtifactSelector::new().the(Attribute::from_str("hardening/value")?))
        .try_collect()
        .await?;
    assert_eq!(rows.len(), probe_values().len(), "every fact must scan");

    for row in rows {
        let from_bytes = row.sort_key()?;
        let from_fields = default_sort_key(&row.to_owned()?);
        assert_eq!(
            from_bytes,
            from_fields,
            "stored-byte and field-derived sort keys diverged for {:?}",
            row.to_owned()?
        );
    }
    Ok(())
}

/// The merge dedup fingerprint rests on the sort key identifying the value
/// exactly: same `(the, of, is)` means same key, and any difference in the
/// value — including a TYPE difference over identical raw bytes — must
/// change it.
#[tokio::test]
async fn it_separates_sort_keys_by_value_identity() -> Result<()> {
    let the = Attribute::from_str("hardening/value")?;
    let of = entity(1);
    let artifact = |is: Value| Artifact {
        the: the.clone(),
        of: of.clone(),
        is,
        cause: None,
    };

    // Same raw payload bytes, different types: must not collide.
    let lookalikes = [
        Value::String("5".into()),
        Value::Bytes(b"5".to_vec()),
        Value::UnsignedInt(5),
    ];
    for (a, left) in lookalikes.iter().enumerate() {
        for (b, right) in lookalikes.iter().enumerate() {
            let equal = default_sort_key(&artifact(left.clone()))
                == default_sort_key(&artifact(right.clone()));
            assert_eq!(
                equal,
                a == b,
                "sort key identity must track value identity: {left:?} vs {right:?}"
            );
        }
    }

    // Identical artifacts agree.
    assert_eq!(
        default_sort_key(&artifact(Value::String("same".into()))),
        default_sort_key(&artifact(Value::String("same".into()))),
    );
    Ok(())
}

/// The URI parse memo must be invisible: a memo hit returns exactly what a
/// fresh parse returns, including `url`'s normalizations, and survives the
/// capacity-clear cycle.
#[tokio::test]
async fn it_memoizes_uri_parses_transparently() -> Result<()> {
    // Normalizing input: `url` appends the trailing slash. The first parse
    // (miss) and second parse (hit) must agree on the normalized form.
    let normalizing = "https://example.com";
    let first = Uri::from_str(normalizing)?;
    let second = Uri::from_str(normalizing)?;
    assert_eq!(first, second);
    assert_eq!(first.to_string(), "https://example.com/");
    assert_eq!(first.key_bytes()?, second.key_bytes()?);

    // Opaque-path entity forms round-trip verbatim.
    for uri in [
        "entity:abcdef",
        "did:key:z6Mk2WiNvjBbuWZ8jYNmFzh4uFyt8iqwpDND6ymg6KnKzchw",
        "did:web:example.org",
    ] {
        let parsed = Uri::from_str(uri)?;
        assert_eq!(parsed.to_string(), uri);
        assert_eq!(Uri::from_str(uri)?, parsed, "hit must equal miss");
    }

    // Failures stay failures on retry (nothing poisonous is cached).
    assert!(Uri::from_str("not a uri with spaces").is_err());
    assert!(Uri::from_str("not a uri with spaces").is_err());

    // Push far past the memo capacity so it clears mid-run, then re-parse
    // an early URI: the post-clear parse must still agree.
    let early = Uri::from_str("entity:early")?;
    for n in 0..5000u32 {
        let _ = Uri::from_str(&format!("entity:cycle-{n}"))?;
    }
    assert_eq!(Uri::from_str("entity:early")?, early);
    Ok(())
}
