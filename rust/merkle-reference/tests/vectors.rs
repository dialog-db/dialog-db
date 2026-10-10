//! Reference values pinned so that a change to the format cannot pass
//! unnoticed. `spec.md` lists the same vectors.

use std::collections::BTreeMap;

use merkle_reference::{Bytes, Integer, Null, Refer, Reference, Type};

fn vectors() -> Vec<(&'static str, Reference)> {
    let mut two_to_the_128 = [0u8; 17];
    two_to_the_128[0] = 1;
    let mut message = BTreeMap::new();
    message.insert("from", "@gozala");
    message.insert("to", "@mikeal");
    message.insert("payload", "Hi");
    vec![
        ("bytes 01020304", Bytes([1u8, 2, 3, 4]).refer()),
        ("null", Null.refer()),
        ("true", true.refer()),
        ("false", false.refer()),
        ("0", 0.refer()),
        ("1985", 1985.refer()),
        ("-1985", (-1985).refer()),
        ("2^128", Integer::new(false, &two_to_the_128).refer()),
        ("18.033", 18.033f64.refer()),
        ("\"hello world\"", "hello world".refer()),
        ("[1, 2, 3]", [1, 2, 3].refer()),
        ("[]", Vec::<bool>::new().refer()),
        ("{from, to, payload}", message.refer()),
        (
            "entity: did:key:alice",
            Type::named("entity:").refer(b"did:key:alice"),
        ),
        (
            "300-byte integer",
            Integer::new(false, &[0xAB; 300]).refer(),
        ),
    ]
}

const PINNED: &[(&str, &str)] = &[
    (
        "bytes 01020304",
        "63781d171425a36312fa058d8712d5d05135a991ec20351ce9d65cdb19a05432",
    ),
    (
        "null",
        "4d619613f9ee44957e317b85876d62717eeefaddb41f5ee68801438ed1c2e4e8",
    ),
    (
        "true",
        "d24b4fe8353c1bc4a7ef19901d3481e63b31c90b37eb2476ef4b17d479446f4c",
    ),
    (
        "false",
        "535caf2484de229f732843678f9fac746e13fa25bf8a420e408a2b67b46872b8",
    ),
    (
        "0",
        "cc5d8e0c2767bfa09919b54ad8e17eb0b8ab6cbbeb5aa42285692bbb562747fe",
    ),
    (
        "1985",
        "414d885d26a2392adcbd2261e08f3625dc2772d4447fdbf557ab3f8836de7920",
    ),
    (
        "-1985",
        "fd8059021fa8e283abcb14e13be2aacd8fa9cbd20b360118d145a4f981534fec",
    ),
    (
        "2^128",
        "db91d4412c3e18440ac8edd0f7f15a2e8c3a0dad5ef6317a6a7c916b061a8061",
    ),
    (
        "18.033",
        "ab68cdd21eee3bf07e204a30201c41de815ebf2dad8b972e9bb56d62fd2be606",
    ),
    (
        "\"hello world\"",
        "c4afdeb66d3e84f3b0826fc3eeace55f9b2417626eb50f7053cf21437c5cfe10",
    ),
    (
        "[1, 2, 3]",
        "56ae0e72e59af3d455399350d3ff76ac93454052e361effa1d7eba916ac1ae3a",
    ),
    (
        "[]",
        "91690ab5f7abf1771f141be0523fcdaa7af82d24d80aee003375cf75c1f67c4c",
    ),
    (
        "{from, to, payload}",
        "cbfcea0f9f6d2f4c571caa4189b2a980507b5b219356370db100a734debb4722",
    ),
    (
        "entity: did:key:alice",
        "9cb91eb8813601e7a8f5a53b993053cc77c77f1e318fc33cef318202d7934213",
    ),
    (
        "300-byte integer",
        "1870673dfeafd85c87f08a86a38d9a124604ed692e597ceec6cb887d5fddf64e",
    ),
];

#[test]
fn it_matches_the_pinned_vectors() {
    let computed = vectors();
    assert_eq!(computed.len(), PINNED.len());
    for ((name, reference), (pinned_name, pinned)) in computed.iter().zip(PINNED) {
        assert_eq!(name, pinned_name);
        assert_eq!(reference.to_string(), *pinned, "{name}");
    }
}
