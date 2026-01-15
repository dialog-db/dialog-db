//! End-to-end test for UCAN-authorized S3 access.
//!
//! This example tests the full flow of using `UcanAuthorizer` with the `Bucket` API:
//!
//! 1. Generate keypairs for a "space" (subject) and "operator"
//! 2. Create a delegation from space to operator
//! 3. Configure `UcanAuthorizer` with the delegation
//! 4. Open a `Bucket` with the authorizer
//! 5. Perform CRUD operations via the access service
//!
//! Run with:
//! ```bash
//! cargo run --example ucan_e2e_test --features ucan -- --service-url <ACCESS_SERVICE_URL>
//! ```
//!
//! For example:
//! ```bash
//! cargo run --example ucan_e2e_test --features ucan -- \
//!     --service-url https://tonk-access-service.xxx.workers.dev
//! ```

use clap::Parser;
use dialog_s3_credentials::ucan::{DelegationChain, OperatorIdentity, UcanAuthorizer};
use dialog_storage::StorageBackend;
use dialog_storage::s3::Bucket;
use ed25519_dalek::SigningKey;
use ucan::delegation::builder::DelegationBuilder;
use ucan::delegation::subject::DelegatedSubject;
use ucan::did::{Ed25519Did, Ed25519Signer};

#[derive(Parser)]
#[command(name = "ucan_e2e_test")]
#[command(about = "End-to-end test for UCAN-authorized S3 access")]
struct Args {
    /// Access service URL (e.g., https://tonk-access-service.xxx.workers.dev)
    #[arg(long)]
    service_url: String,

    /// Test content (default: "Hello, UCAN!")
    #[arg(long, default_value = "Hello from dialog-storage UCAN test!")]
    content: String,

    /// Skip cleanup (leave test data in storage)
    #[arg(long)]
    skip_cleanup: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("=== UCAN E2E Test ===\n");
    println!("Service URL: {}", args.service_url);

    // Step 1: Generate keypairs
    println!("\n[1/6] Generating keypairs...");

    let mut space_seed = [0u8; 32];
    getrandom::getrandom(&mut space_seed)?;
    let space_signing_key = SigningKey::from_bytes(&space_seed);
    let space_signer = Ed25519Signer::new(space_signing_key);
    let space_did = space_signer.did();
    println!("  Space DID: {}", space_did);

    let mut operator_seed = [0u8; 32];
    getrandom::getrandom(&mut operator_seed)?;
    let operator_identity = OperatorIdentity::from_secret(&operator_seed);
    println!("  Operator DID: {}", operator_identity.did());

    // Step 2: Create delegation (space -> operator)
    println!("\n[2/6] Creating delegation...");

    // Parse operator DID for delegation audience
    let operator_did: Ed25519Did = operator_identity
        .did()
        .to_string()
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse operator DID: {:?}", e))?;

    let delegation = DelegationBuilder::new()
        .issuer(space_signer.clone())
        .audience(operator_did)
        .subject(DelegatedSubject::Specific(*space_did))
        .command(vec!["http".to_string()]) // Delegate all http/* commands
        .try_build()
        .map_err(|e| anyhow::anyhow!("Failed to build delegation: {:?}", e))?;

    let delegation_bytes = serde_ipld_dagcbor::to_vec(&delegation)?;
    let delegation_cid = delegation.to_cid();
    println!("  Delegation CID: {}", delegation_cid);

    // Step 3: Configure UcanAuthorizer
    println!("\n[3/6] Configuring UcanAuthorizer...");

    let delegation_chain = DelegationChain::single(delegation_bytes, delegation_cid);

    let authorizer = UcanAuthorizer::builder()
        .service_url(&args.service_url)
        .operator(operator_identity)
        .delegation(space_did.to_string(), delegation_chain)
        .build()?;

    println!("  Authorizer configured with 1 delegation");

    // Step 4: Open bucket and perform operations
    println!("\n[4/6] Opening bucket...");

    let bucket: Bucket<Vec<u8>, Vec<u8>> = Bucket::open(authorizer)?;

    // Scope to subject's index
    let subject_path = format!("{}/index", space_did);
    let mut backend = bucket.at(&subject_path);
    println!("  Bucket opened at path: {}", subject_path);

    // Step 5: Test CRUD operations
    println!("\n[5/6] Testing CRUD operations...");

    // Generate unique test key using timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let test_key = format!("test-key-{}", timestamp).into_bytes();
    let test_value = args.content.as_bytes().to_vec();

    println!("  Test key: {}", String::from_utf8_lossy(&test_key));
    println!("  Test value: {} bytes", test_value.len());

    // PUT
    println!("\n  [PUT] Writing value...");
    backend.set(test_key.clone(), test_value.clone()).await?;
    println!("  [PUT] Success!");

    // GET
    println!("\n  [GET] Reading value...");
    let retrieved = backend.get(&test_key).await?;
    match retrieved {
        Some(value) if value == test_value => {
            println!("  [GET] Success! Content matches.");
        }
        Some(value) => {
            println!("  [GET] FAILED! Content mismatch.");
            println!("    Expected: {:?}", String::from_utf8_lossy(&test_value));
            println!("    Got: {:?}", String::from_utf8_lossy(&value));
            return Err(anyhow::anyhow!("Content verification failed"));
        }
        None => {
            println!("  [GET] FAILED! Value not found.");
            return Err(anyhow::anyhow!("Value not found after PUT"));
        }
    }

    // GET nonexistent key
    println!("\n  [GET] Testing nonexistent key...");
    let nonexistent_key = b"nonexistent-key-12345".to_vec();
    let result = backend.get(&nonexistent_key).await?;
    if result.is_none() {
        println!("  [GET] Success! Returns None for nonexistent key.");
    } else {
        println!("  [GET] FAILED! Expected None for nonexistent key.");
        return Err(anyhow::anyhow!("Expected None for nonexistent key"));
    }

    // Step 6: Cleanup (DELETE)
    if !args.skip_cleanup {
        println!("\n[6/6] Cleaning up...");
        backend.delete(&test_key).await?;
        println!("  [DELETE] Success!");

        // Verify deletion
        let deleted = backend.get(&test_key).await?;
        if deleted.is_none() {
            println!("  [VERIFY] Key successfully deleted.");
        } else {
            println!("  [VERIFY] WARNING: Key still exists after delete.");
        }
    } else {
        println!("\n[6/6] Skipping cleanup (--skip-cleanup)");
    }

    println!("\n=== All Tests Passed! ===\n");

    // Print test artifacts for debugging
    println!("Test artifacts (for debugging):");
    println!(
        "  Space seed (base64): {}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &space_seed)
    );
    println!(
        "  Operator seed (base64): {}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &operator_seed)
    );

    Ok(())
}
