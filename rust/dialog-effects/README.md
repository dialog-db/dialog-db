# dialog-effects

Capability hierarchy types for Dialog-DB.

Defines the domain-specific attenuations, policies, and effects that form capability chains. Each module corresponds to a capability domain.

## access

Authorization and delegation.

```text
Subject (profile DID)
└── Access
    ├── Prove { principal, access, duration } → Proof
    └── Retain { delegation } → ()
```

## storage

Bootstrap space operations. Used during setup before the operator is built.

```text
Subject (did:local:storage)
└── Storage
    └── Location { directory, name }
        ├── Load → Credential
        └── Create { credential } → Credential
```

## space

Operator-level space operations. Used after bootstrap to open repositories.

```text
Subject (profile DID)
└── Space { name }
    ├── Load → Credential
    └── Create { credential } → Credential
```

## archive

Content-addressed block storage.

```text
Subject (repository DID)
└── Archive
    └── Catalog { name }
        ├── Get { digest } → Option<Vec<u8>>
        └── Put { digest, content } → ()
```

## memory

Transactional memory cells for branch state.

```text
Subject (repository DID)
└── Memory
    └── Space { name }
        └── Cell { name }
            ├── Resolve → Option<Vec<u8>>
            ├── Publish { content } → ()
            └── Retract → ()
```

## credential

Credential read/write for identity persistence.

```text
Subject (did:local:storage)
└── Credential
    └── Address { address }
        ├── Load → Credential
        └── Save { credential } → ()
```

Effects are structural types only. Storage providers in `dialog-storage` implement `Provider<Fx>` for each effect.
