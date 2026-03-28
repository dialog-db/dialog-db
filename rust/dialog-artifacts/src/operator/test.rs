#[cfg(test)]
mod tests {
    use crate::profile::Profile;
    use crate::remote::Remote;
    use dialog_capability::storage::Storage;
    use dialog_storage::provider::FileSystem;

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let profile = Profile::named("personal")
            .open(Storage::temp())
            .perform(&FileSystem)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .storage(FileSystem)
            .network(Remote)
            .mount(Storage::temp())
            .build()
            .await
            .unwrap();

        assert_ne!(
            operator.profile_did(),
            operator.did(),
            "profile and operator DIDs should differ"
        );
    }

    #[dialog_common::test]
    async fn operator_key_is_deterministic() {
        let location = Storage::temp();

        let profile = Profile::named("work")
            .open(location.clone())
            .perform(&FileSystem)
            .await
            .unwrap();

        let op1 = profile
            .operator(b"alice")
            .storage(FileSystem)
            .network(Remote)
            .mount(Storage::temp())
            .build()
            .await
            .unwrap();

        let op2 = profile
            .operator(b"alice")
            .storage(FileSystem)
            .network(Remote)
            .mount(Storage::temp())
            .build()
            .await
            .unwrap();

        assert_eq!(
            op1.did(),
            op2.did(),
            "same context should produce same operator DID"
        );
    }

    #[dialog_common::test]
    async fn different_contexts_produce_different_operators() {
        let location = Storage::temp();

        let profile = Profile::named("work")
            .open(location.clone())
            .perform(&FileSystem)
            .await
            .unwrap();

        let alice = profile
            .operator(b"alice")
            .storage(FileSystem)
            .network(Remote)
            .mount(Storage::temp())
            .build()
            .await
            .unwrap();

        let bob = profile
            .operator(b"bob")
            .storage(FileSystem)
            .network(Remote)
            .mount(Storage::temp())
            .build()
            .await
            .unwrap();

        assert_ne!(
            alice.did(),
            bob.did(),
            "different contexts should produce different operator DIDs"
        );
    }
}
