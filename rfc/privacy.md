# DialogDB Privacy RFC

## Design Goals

DialogDB's privacy model is designed with the following goals:

1. **Full Infrastructure Privacy**: Storage and coordination infrastructure should have no access to user data
2. **Tiered Access Levels**: Different actors can be granted different levels of access
3. **Selective Sharing**: Fine-grained control over which parts of the database are shared
4. **Authorization Integration**: Support for capability-based authorization systems like UCAN
5. **Privacy-Efficiency Tradeoffs**: Allow users to choose their preferred balance

## Access Levels

DialogDB implements a layered privacy model with distinct access levels:

```mermaid
flowchart TD
    subgraph "Access Levels"
        direction TB
        L0["Level 0: No Access<br/>Complete Opacity"]
        L1["Level 1: Structure Access<br/>Blob Connectivity"]
        L2["Level 2: Range Access<br/>Tree Navigation"]
        L3["Level 3: Data Access<br/>Content Visibility"]
    end
    
    L0 --> L1 --> L2 --> L3
    
    style L0 fill:#ddd,stroke:#333,stroke-width:1px
    style L1 fill:#bfb,stroke:#333,stroke-width:1px
    style L2 fill:#bbf,stroke:#333,stroke-width:1px
    style L3 fill:#f9f,stroke:#333,stroke-width:1px
```

### Level 0: No Access

The base infrastructure level:

- **Blob Store**: Stores fully encrypted, opaque blobs without any knowledge of their content or relationships
- **Mutable Pointer**: Manages references without knowledge of what is being referenced
- **No Data Visibility**: Cannot see or derive any value from user data
- **Authorization Only**: Access only through proper authorization credentials

At this level, infrastructure components are completely unaware of the data, only handling completely opaque, encrypted content. This ensures that infrastructure providers cannot derive any value from user data or metadata.

### Level 1: Structure Access

Limited structural visibility:

- **Blob Traversal**: Authorized actors can follow links between blobs
- **Sync Assistance**: Can help with synchronization by providing missing connected nodes
- **No Content Insight**: Still cannot see data content or key ranges
- **Minimal Metadata**: Only visibility is blob connectivity

This level enables efficient synchronization help - an L1 actor can bundle all connected nodes a user is missing into a single compressed package, improving transfer efficiency without revealing data content.

### Level 2: Range Access

Tree structure visibility:

- **Key Range Awareness**: Can see key ranges to verify proper tree structure
- **Tree Validation**: Can validate the consistency of the Probabilistic B-Tree
- **Range-Based Retrieval**: Can retrieve subtrees covering specific key ranges
- **No Content Access**: Still cannot see actual fact values

L2 access reveals some metadata (key distribution) but enables significant efficiency gains, like fetching specific subtrees for selective replication. It allows for server-side validation of tree structure while keeping content private.

### Level 3: Data Access

Content visibility with group-based access:

- **Fact Decryption**: Can decrypt and view actual facts
- **Local Querying**: Enables full query capabilities against the database
- **Group-Based Encryption**: Different facts can be encrypted for different access groups
- **Selective Visibility**: Members of one group cannot see facts encrypted for other groups

This level is primarily for collaborators who need to see and work with the actual data. Different facts within the database can be encrypted for different groups, providing fine-grained access control within the L3 level itself.

## Authorization Model

DialogDB supports capability-based authorization through UCANs (User Controlled Authorization Networks):

```mermaid
flowchart TD
    subgraph "UCAN Authorization"
        Alice["Alice<br/>Data Owner"] -->|"Delegates<br/>L1 access"| SyncService["Sync Service"]
        Alice -->|"Delegates<br/>L2 access"| ValidationService["Validation Service"]
        Alice -->|"Delegates<br/>L3 access"| Bob["Bob<br/>Collaborator"]
        Alice -->|"Delegates<br/>L3 access<br/>(Group A only)"| Charlie["Charlie<br/>Limited Collaborator"]
    end
    
    style Alice fill:#f96,stroke:#333,stroke-width:2px
    style SyncService fill:#bfb,stroke:#333,stroke-width:1px
    style ValidationService fill:#bbf,stroke:#333,stroke-width:1px
    style Bob fill:#f9f,stroke:#333,stroke-width:1px
    style Charlie fill:#fcc,stroke:#333,stroke-width:1px
```

### UCAN Integration

- **Capability-Based**: Authorization uses "bearer tokens" that encode specific capabilities
- **Delegation Chain**: Capabilities can be delegated from users to services or other users
- **Proofs**: Authorization can be cryptographically verified
- **In-Tree Storage**: Delegation tokens can be stored within the tree structure itself
- **Parent-Child Authorization**: Parent nodes can contain delegations for their children

UCANs enable a powerful authorization model where:

1. The owner of a database can issue capabilities to others
2. Those capabilities can be precisely scoped to specific access levels and subtrees
3. Capabilities can be delegated further with equal or more restrictive permissions
4. Access can be verified without a central authority

## Implementation Architecture

The multi-layered encryption approach is implemented through a nested structure:

```mermaid
flowchart TD
    subgraph "Node Encryption Architecture"
        direction TB
        Outer["Outer Layer (L1)<br/>Blob Connectivity"] --> Middle["Middle Layer (L2)<br/>Key Ranges"]
        Middle --> Inner["Inner Layer (L3)<br/>Fact Values"]
    end
    
    style Outer fill:#bfb,stroke:#333,stroke-width:2px
    style Middle fill:#bbf,stroke:#333,stroke-width:2px
    style Inner fill:#f9f,stroke:#333,stroke-width:1px
```

### Tiered Encryption Implementation

1. **Level 3 Encryption (Inner Layer)**
   - Encrypts the actual fact values
   - Can use different encryption keys for different fact groups
   - Only accessible to authorized collaborators

2. **Level 2 Encryption (Middle Layer)**
   - Encrypts key range information
   - Wraps the L3-encrypted content
   - Enables tree structure validation

3. **Level 1 Encryption (Outer Layer)**
   - Encrypts child references
   - Wraps the L2-encrypted content
   - Enables basic blob connectivity traversal

### Key Derivation and Management

```mermaid
flowchart TD
    Root["Root Key"] --> L1["L1 Access Keys"]
    Root --> L2["L2 Access Keys"]
    Root --> L3["L3 Access Keys"]
    
    L3 --> GroupA["Group A Keys"]
    L3 --> GroupB["Group B Keys"]
    L3 --> GroupC["Group C Keys"]
    
    style Root fill:#f96,stroke:#333,stroke-width:2px
    style L1 fill:#bfb,stroke:#333,stroke-width:1px
    style L2 fill:#bbf,stroke:#333,stroke-width:1px
    style L3 fill:#f9f,stroke:#333,stroke-width:1px
```

- **Hierarchical Keys**: Access keys are derived from a root key
- **Group-Specific Keys**: Within L3, different groups can have different keys
- **Key Distribution**: Keys are securely distributed to authorized parties
- **Key Rotation**: Support for key rotation without rebuilding the entire tree

## Privacy Tradeoffs and User Choice

A core principle of DialogDB is that users should be able to choose their own privacy-efficiency tradeoffs:

```mermaid
flowchart LR
    subgraph "Privacy-Efficiency Spectrum"
        direction TB
        P1["Maximum Privacy<br/>L0 access only"] --> P2["High Privacy<br/>Limited L1"] 
        P2 --> P3["Balanced<br/>Selective L2"] --> P4["Efficiency-Focused<br/>Broad L2 & L3"]
    end
    
    style P1 fill:#f9f,stroke:#333,stroke-width:1px
    style P2 fill:#bbf,stroke:#333,stroke-width:1px
    style P3 fill:#bfb,stroke:#333,stroke-width:1px
    style P4 fill:#fcc,stroke:#333,stroke-width:1px
```

Users can configure their database to operate anywhere on this spectrum:

- **Maximum Privacy**: Everything fully encrypted, limited delegation
- **Balanced Approach**: Strategic delegation of L1/L2 access to trusted services
- **Collaboration Focus**: Broader sharing with appropriate access controls
- **Different Regions**: Apply different policies to different subtrees

## Practical Use Cases

### Private Cloud Sync

```mermaid
sequenceDiagram
    participant User as User
    participant Local as Local DB
    participant Sync as Sync Service (L1)
    participant Blob as Blob Store (L0)
    
    User->>Local: Update data
    Local->>Blob: Store encrypted nodes
    Local->>Sync: Request sync
    Sync->>Blob: Fetch connected nodes
    Sync->>Local: Send missing nodes
    Local->>Local: Verify & decrypt
```

The sync service can help transfer connected nodes without seeing their content.

### Collaborative Editing with Group Access

```mermaid
sequenceDiagram
    participant Alice as Alice (Owner)
    participant Bob as Bob (Group A)
    participant Charlie as Charlie (Group B)
    participant Validator as Validator (L2)
    
    Alice->>Alice: Create facts for Groups A & B
    Alice->>Bob: Share Group A access key
    Alice->>Charlie: Share Group B access key
    Alice->>Validator: Delegate L2 access
    
    Bob->>Validator: Push changes (Group A)
    Validator->>Validator: Verify tree structure
    
    Charlie->>Validator: Push changes (Group B)
    Validator->>Validator: Verify tree structure
    
    Bob->>Bob: Query & decrypt Group A facts
    Charlie->>Charlie: Query & decrypt Group B facts
```

Different collaborators can work on different parts of the database with appropriate access.

## Security Considerations

1. **Key Management**: Secure storage and rotation of encryption keys
2. **Authorization Validation**: Proper validation of UCAN chains
3. **Side-Channel Attacks**: Protection against metadata leakage
4. **Forward Secrecy**: Key rotation strategies
5. **Revocation**: Methods to revoke delegated access

## Next Steps

1. Implement UCAN integration for authorization
2. Develop key management protocols
3. Create reference implementations of multi-layer encryption
4. Design user interfaces for managing access levels and delegations
5. Benchmark performance across different privacy configurations