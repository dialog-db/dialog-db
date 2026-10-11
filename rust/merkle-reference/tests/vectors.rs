//! Reference values pinned so that a change to the format cannot pass
//! unnoticed. `spec.md` lists the same vectors.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use merkle_reference::{Reference, Type};

fn text(value: &str) -> Reference {
    Type::named("text:").unwrap().reference_of(value.as_bytes())
}

fn vectors() -> Vec<(&'static str, Reference)> {
    let entries = [("from", "@gozala"), ("to", "@mikeal"), ("payload", "Hi")];
    vec![
        ("bytes 01020304", Reference::hash(&[1, 2, 3, 4])),
        ("type text:", Type::named("text:").unwrap().reference()),
        ("text: \"hello world\"", text("hello world")),
        ("text: \"\"", text("")),
        (
            "entity: did:key:alice",
            Type::named("entity:")
                .unwrap()
                .reference_of(b"did:key:alice"),
        ),
        (
            "list: [\"a\", \"b\", \"c\"]",
            Type::LIST.list(["a", "b", "c"].map(text)),
        ),
        ("list: []", Type::LIST.list([])),
        (
            "map: {from, to, payload}",
            Type::MAP.map(entries.map(|(key, value)| (text(key), text(value)))),
        ),
        ("map: {}", Type::MAP.map([])),
        (
            "set: {\"a\", \"b\", \"c\"}",
            Type::SET.set(["c", "a", "b"].map(text)),
        ),
        ("set: {}", Type::SET.set([])),
    ]
}

const PINNED: &[(&str, &str)] = &[
    (
        "bytes 01020304",
        "63781d171425a36312fa058d8712d5d05135a991ec20351ce9d65cdb19a05432",
    ),
    (
        "type text:",
        "ab31fce38517675ffb0055997c0e925eaa057459986eb71969a3e914bfc06f87",
    ),
    (
        "text: \"hello world\"",
        "9c8321ef7176250ce8c036545f034d40a0e00d9d2fa2eccf4d4978e993cab657",
    ),
    (
        "text: \"\"",
        "695633a4ac641447b3ea93ef43b0b2445b879324a7c88fab1ee09d370d6a7313",
    ),
    (
        "entity: did:key:alice",
        "9cb91eb8813601e7a8f5a53b993053cc77c77f1e318fc33cef318202d7934213",
    ),
    (
        "list: [\"a\", \"b\", \"c\"]",
        "a9f5c01fa32908502ba0f2485a8da7764b5d8e5460f6159167678f81d6c30092",
    ),
    (
        "list: []",
        "10fc213d5e98202cf24d703ca1eb188a1620d49206822f4560efedcd24dc645c",
    ),
    (
        "map: {from, to, payload}",
        "befa44ef2225f206a213ae0e8f54bc36e85c0b2180cecc576f13ec3c7cf234b2",
    ),
    (
        "map: {}",
        "15401fcccef0b6088e064e0583d32bca06e3204522df130b032d434a6a9f2c69",
    ),
    (
        "set: {\"a\", \"b\", \"c\"}",
        "91bd53901c8ce0bf5119289067f87c16c45b6e2a50a1bec7c0ab00d55b93a827",
    ),
    (
        "set: {}",
        "42d6ea87f2aac355034b6c00ab0703ad59ba8fe28cff3f0f1c91229c95b912be",
    ),
];

#[dialog_common::test]
fn it_matches_the_pinned_vectors() {
    let computed = vectors();
    assert_eq!(computed.len(), PINNED.len());
    for ((name, reference), (pinned_name, pinned)) in computed.iter().zip(PINNED) {
        assert_eq!(name, pinned_name);
        assert_eq!(reference.to_string(), *pinned, "{name}");
    }
}
