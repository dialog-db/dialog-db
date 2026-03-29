#[cfg(test)]
mod tests {
    use crate::profile::Profile;
    use crate::remote::Remote;
    use dialog_capability::Capability;
    use dialog_capability::storage::{Location, Storage};
    use dialog_storage::provider::{Address, FileSystem, Store, fs};

    fn temp_location() -> Capability<Location<Address>> {
        let address = fs::Address::temp().resolve(&unique_id()).unwrap();
        Storage::locate(Address::FileSystem(address))
    }

    fn temp_store() -> Store {
        let address = fs::Address::temp().resolve(&unique_id()).unwrap();
        Store::FileSystem(FileSystem::mount(&address).unwrap())
    }

    fn unique_id() -> String {
        use dialog_common::time;
        format!(
            "dialog-{}",
            time::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
    }

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let profile = Profile::named("personal")
            .open(temp_location())
            .perform(&FileSystem)
            .await
            .unwrap();

        let operator = profile
            .operator(temp_store(), b"alice")
            .network(Remote)
            .build()
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
        let location = temp_location();

        let profile = Profile::named("work")
            .open(location)
            .perform(&FileSystem)
            .await
            .unwrap();

        let op1 = profile
            .operator(temp_store(), b"alice")
            .network(Remote)
            .build()
            .await
            .unwrap();

        let op2 = profile
            .operator(temp_store(), b"alice")
            .network(Remote)
            .build()
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
        let location = temp_location();

        let profile = Profile::named("work")
            .open(location)
            .perform(&FileSystem)
            .await
            .unwrap();

        let alice = profile
            .operator(temp_store(), b"alice")
            .network(Remote)
            .build()
            .await
            .unwrap();

        let bob = profile
            .operator(temp_store(), b"bob")
            .network(Remote)
            .build()
            .await
            .unwrap();

        assert_ne!(
            alice.authority.operator_did(),
            bob.authority.operator_did(),
            "different contexts should produce different operator DIDs"
        );
    }
}
