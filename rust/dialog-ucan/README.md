# dialog-ucan

UCAN authorization protocol for Dialog-DB.

Bridges `dialog-capability`'s generic access protocol with `dialog-ucan-core`'s UCAN spec implementation. Defines how UCAN delegation chains are used to prove and delegate access.

## Usage

```rust
// Delegate repo access from Alice to Bob
let delegation = alice_profile.access()
    .claim(&repo)
    .delegate(bob_profile.did())
    .perform(&alice_operator)
    .await?;

// Bob retains the delegation
bob_profile.access()
    .save(delegation)
    .perform(&bob_operator)
    .await?;

// Capability to access archive's "index" catalog
let capability = repo
    .subject()
    .archive()
    .catalog("index");

let chain = alice_profile.access()
    .claim(capability)
    .delegate(bob_profile.did())
    .perform(&alice_operator)
    .await?;
```
