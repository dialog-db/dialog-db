#[cfg(test)]
mod tests {
    use crate::profile::Profile;
    use crate::remote::Remote;
    use crate::storage::Storage;

    fn unique_name(prefix: &str) -> String {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{seq}")
    }

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("build")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(operator.profile_did(), operator.did());
    }

    #[dialog_common::test]
    async fn operator_key_is_deterministic() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("det")))
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

        assert_eq!(op1.did(), op2.did());
    }

    #[dialog_common::test]
    async fn different_contexts_produce_different_operators() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("ctx")))
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

        assert_ne!(alice.did(), bob.did());
    }

    #[dialog_common::test]
    async fn end_to_end_profile_operator_repository() {
        use dialog_capability::Subject;
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
        use dialog_effects::memory::prelude::{MemoryExt, SubjectExt as MemorySubjectExt};

        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("e2e")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .allow(Subject::any().archive().catalog("index"))
            .allow(Subject::any().archive().catalog("content"))
            .allow(Subject::any().memory().space("local"))
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        let home = crate::Repository::open(Storage::temp(&unique_name("home")))
            .perform(&operator)
            .await
            .unwrap();

        assert!(!home.did().to_string().is_empty());
        assert_ne!(profile.did(), home.did());
        assert!(operator.storage().stores().contains(&home.did()));
    }

    #[dialog_common::test]
    async fn powerline_delegation() {
        use dialog_capability::Subject;

        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("power")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"admin")
            .allow(Subject::any())
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(profile.did(), operator.did());
    }
}
