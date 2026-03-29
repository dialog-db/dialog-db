#[cfg(test)]
mod tests {
    use crate::profile::Profile;
    use crate::remote::Remote;
    use crate::storage::Storage;

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp("test"))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(
            operator.authority.profile_did(),
            operator.authority.operator_did(),
            "profile and operator DIDs should differ"
        );
    }

    #[dialog_common::test]
    async fn operator_key_is_deterministic() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp("test"))
            .perform(&storage)
            .await
            .unwrap();

        let op1 = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let op2 = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_eq!(
            op1.authority.operator_did(),
            op2.authority.operator_did(),
            "same context should produce same operator DID"
        );
    }

    #[dialog_common::test]
    async fn different_contexts_produce_different_operators() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp("test"))
            .perform(&storage)
            .await
            .unwrap();

        let alice = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let bob = profile
            .operator(b"bob")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(
            alice.authority.operator_did(),
            bob.authority.operator_did(),
            "different contexts should produce different operator DIDs"
        );
    }
}
