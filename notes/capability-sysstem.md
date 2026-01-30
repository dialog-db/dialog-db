# Capability System

Roc language offers [clear separation between platform and application](https://www.roc-lang.org/platforms) code. If you squint a bit, you may notice it is not too different from [Algebraic Effects  in Unison](https://www.unison-lang.org/docs/language-reference/abilities-and-ability-handlers/) and I would say it carries Elm's pragmatic approach of reducing flexibility for the sake of simplicity and understandability. Effects are essentially [commands](https://package.elm-lang.org/packages/elm/core/latest/Platform.Cmd) or a way for an **application** to tell to a **platform**  *“Hey, I want you to do this thing!”*. In order for this to work **platform** needs to provide corresponding [capability](https://github.com/ucan-wg/spec?tab=readme-ov-file#capability) so that **application** can [invoke][invocation] it.

This is relevant for dialog for two reasons

1. We already have platform code separation in form of various storage backend implementations.
2. [Object capability model](https://en.wikipedia.org/wiki/Object-capability_model) is only viable way to manage access in non-centralized potentially offline settings. 

## Proposed Design Sketch

Define a set of platform capabilities (as replacement for backends) that can be provided by various **environments** _(our term for platform)_  making application code [referentially transparent](https://en.wikipedia.org/wiki/Referential_transparency), environment agnostic and straightforward to test and embed. Here is the illustration.

> A capability is the association of an ability to a subject: `subject x command x policy`.

### Subject

Every capability is bound to specific **subject** resource, complete unrestricted access to it can be denoted by unconstrained delegation. In dialog such resource is a repository identified by [did:key][] identifier

```rust
/// Repository access is unconstrained by anithing
/// other than subject repository did
type RepositoryAccess = Subject;
type Repository = Delegation<RepositoryAccess>;
```

The `Repository` corresponds and can be even serialized as a following [UCAN Delegation][delegation] 

```json
{
  "cmd": "/",
  "sub": "did:key:zSpace",
  "pol": [],
  
  "iss": "did:key:zAlice",
  "aud": "did:key:zBob",
  "exp": null
}
```

### Ability

Capability can be constrained by restricting access to specific abilities. For example below we define `Archive` that restricts commands that can be invoked

```rust
/// Constraint that adds archive to the cmd path by implementing
/// Ability trait
struct Archive;
impl Ability for Archive {}

type ArchiveAccess = Access<Archive, RepositoryAccess>;
type RepositoryArchive = Delegation<ArchiveAccess>;
```

Above delegation restricts repository access by `archive` ability and it will correspond to following [UCAN Delegation][delegation]

```json
{
  "cmd": "/archive",
  "sub": "did:key:zSpace",
  "pol": [],
  
  "iss": "did:key:zAlice",
  "aud": "did:key:zBob",
  "exp": null
}
```

### Policy

Capability can be restricted although through specific policies. For example we can define `Catalog` policy that restricts invocation arguments

```rust
struct Catalog {
  /// restrict archive access by catalog
  catalog: String
}
impl Policy for Catalog {}

type CatalogAccess = Access<Catalog, ArchiveAccess>;
type ArchiveCatalog = Delegation<CatalogAccess>;
```

Above delegation restricts archive access by `catalog` and it will correspond to following [UCAN Delegation][delegation]

```rust
{
  "cmd": "/archive",
  "sub": "did:key:zSpace",
  "pol": [
    ["==", ".catalog", "index"]
  ],
  
  "iss": "did:key:zAlice",
  "aud": "did:key:zBob",
  "exp": null
}
```

### Effect

Effects are capabilities that can be invoked, like you can have access to an archive or catalog within it, but those are not things that you can invoked. When effects are performed by the host environment they produce `output`s which is what 

```rust
pub struct Get {
    pub key: Vec<u8>
}
impl Ability for Get {}

type DoGet = Access<Get, CatalogAccess>;
impl Effect for DoGet {
    type Output = Result<Vec<u8>, GetError>;
}

type GetInvocation = Invocation<DoGet>;
```

### Provider

In order to perform effects environment need to provide implementations for them, which implies implementing a `Provider` trait as shown below

```rust
// Demo environment for illustration
struct DemoEnv;

impl Provider<DoGet> for DemoEnv {
    // validated invocation will is passed to the provider to perform an effect
    async fn execute(&mut self, invocation: GetInvocation) -> <DoGet as Invocation>::Output {
			  assert_eq!(get.command(), "/archive/get");
      
        print!("subject: {}", get.subject());
			  print!("catalog: {}", Catalog::policy(&invocation).catalog);
				print!("key {}", Get::policy(&invocation).key);
      
        Ok(b"world")
   }
}
```

## Composition

All these pieces can be composed together into convenient DSL

```rust
let capability = Subject::from("did:key:zSpace") // Claim<RepositoryAccess>
   .claim(Archive) // Claim<ArchiveAccess>
   .claim(Catalog { catalog: "index".into() }); // Claim<CatalogAccess>

// can access associated subject
assert_eq!(capability.subject(), "did:key:zSpace".into());

// can access associated abilities
assert_eq!(capability.ability(), "/archive");

// can access associated policies
assert_eq!(Catalog::policy(&capability).catalog, "index");
```

Effectful functions can acquire desired capabilities from the host runtime. They describe environment in terms of capabilities provider must provide in the example below environment must provide `Acquire` , and `DoGet` capabilities. `Acquire` is used to get delegation for request capability to current operator, `DoGet` is used to perform `get` effect.

```rust
async fn demo <Env> (&mut env: Env) -> Result<(), AuthorizationError>
where 
  Env: Provider<Acquire<CatalogAccess>> + Provider<DoGet> 
{
  let catalog = capability
  			// will require catalog capability to be delegated to the
  			// currently active principal
        .acquire(env)
				// authorization may fail if no delegation can not be
  			// arranged for the active provider
        .await?;
  
  let get = catalog.invoke(Get { key: b"hello" }); // GetInvocation
  assert_eq!(get.subject(), "did:key:zSpace".into());
  assert_eq!(get.command(), "/archive/get");
  assert_eq!(Catalog::policy(&get).catalog, "index");
  assert_eq!(Get::policy(&get).key, b"hello");
  
  // Ask enviroment to perform get effect and give us output
  let content = get.perform(env).await?;
  println!("{:?}", content);
  
  Ok(())
}
```

Note that `Env` parameter does not necessarily need to list every single effect that will be used instead you can expect ability groups that can be defined using rust traits like

```rust
trait Archive: Provider<DoGet> + Provider<DoPut> {}
impl <T: Provider<DoGet> + Provider<DoPut>> Archive for T {}

trait Memory: Provider<DoResolve> + Provider<DoPublish> {}
impl <T: Provider<DoResolve> + Provider<DoPublish>> Memory for T {}

trait Storage: Archive + Memory {}
impl <T: Archive + Memory> Storage for T {}
```



## Proposed Capabilities

```rust
type RepositoryAccess = Subject;

#[derive(Ability)]
struct Archive;
type ArchiveAccess = Access<Archive, RepositoryAccess>;

#[derive(Policy)]
struct Catalog {
  /// restrict archive access by catalog
  pub catalog: String
}

type CatalogAccess = Access<Catalog, ArchiveAccess>;

/// Retrieves content corresponding to the requested digest
#[derive(Ability)]
pub struct Get {
	pub digest: Blake3Hash;
}

impl Effect for Access<Get, CatalogAccess> {
    type Output = Result<Vec<u8>, ArchiveError>;
}

/// Stores given content in the archive
#[derive(Ability)]
pub struct Put {
  pub digest: Blake3Hash,
  pub content: Vec<u8>
}
impl Effect for Access<Put, CatalogAccess> {
  type Output = Result<(), ArchiveError>;
}

#[derive(Ability)]
type Memory;
type MemoryAccess = Access<Memory, RepositoryAccess>;

#[derive(Policy)]
type MemorySpace {
  /// Restricts access to a specific memory space
  pub memory: String;
}
type MemorySpaceAccess = Access<MemorySpace, MemoryAccess>;

#[derive(Policy)]
type Cell {
  /// Restricts access to a specific memory cell
  pub cell: String;
}
type CellAccess = Access<Cell, MemorySpace>;

type Edition = Vec<u8>;
type Content = Vec<u8>;

#[derive(Ability)]
pub struct Resolve;
/// Resolves memory cell returning it's current content
/// and associated edition. If cell has not content returns
/// None
impl Effect for Access<Resolve, CellAccess> {
  type Output = Result<Option<(Content, Edition)>, MemoryError>;
}

#[derive(Ability)]
pub struct Publish {
  /// Content to publish to a cell. If set to None
  /// implies unpublishing.
  pub content: Option<Vec<u8>>;
  /// currently expected edition. If no content is
  /// expected set to None.
  pub edition: Option<Vec<u8>>;
}
/// Updates cell content with CAS semantics. If edition in the
/// cell is different from expected produced an error. Otherwise
/// returns new edition. If content published is None returns
/// None impliyng cell having no edition.
impl Effect for Access<Publish, CellAccess> {
  type Output = Result<Option<Vec<u8> MemoryError>
}


#[derive(Ability)]
pub struct Capability;
pub type CapabilityAccess = Access<Capability, Subject>;


#[derive(Authorize)]
pub struct Acquire<Access> {
  access: Claim<Access>
}
/// Aquires delegation for a request capability
impl Effect for Access<Acquire, CapabilityAccess> {
  type Output = Result<Delegation<Access>, AuthorizationError>;
}
```





[delegation]:https://github.com/ucan-wg/delegation
[invocation]:https://github.com/ucan-wg/invocation
[did:key]:https://w3c-ccg.github.io/did-key-spec/
