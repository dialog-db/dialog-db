# dialog-operator

Operator layer for Dialog-DB.

This crate provides the capability-based operator system: authority credentials,
profiles, operator builders, storage dispatch, and remote fork dispatch that
together form the operational layer above the core artifact store.

## Modules

- **authority** -- Opened profile with signers and authority chain
- **profile** -- Named identity with signing credential
- **operator** -- Operating environment built from a profile
- **storage** -- DID-routed storage dispatcher
- **remote** -- Remote dispatch for fork invocations
- **helpers** -- Test helpers (behind the `helpers` feature flag)
