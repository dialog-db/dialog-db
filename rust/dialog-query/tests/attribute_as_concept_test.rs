//! Test that attributes implement the Concept trait

use dialog_query::concept::Instance;
use dialog_query::{Concept, Entity, Match, Term};

mod note {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Title(pub String);

    #[derive(Attribute, Clone)]
    pub struct Body(pub String);
}

#[test]
fn test_attribute_implements_concept() {
    // With<Attribute> should implement Concept
    // This is a compile-time test - if it compiles, the trait is implemented
    fn assert_is_concept<T: Concept>() {}

    // With<Title> and With<Body> should implement Concept
    assert_is_concept::<dialog_query::attribute::With<note::Title>>();
    assert_is_concept::<dialog_query::attribute::With<note::Body>>();

    println!("✓ With<Attribute> types implement Concept trait!");
}

#[test]
fn test_attribute_concept_const() {
    use dialog_query::predicate::concept::Concept as ConceptPredicate;

    // Access the CONCEPT const from With<Attribute>
    let concept: ConceptPredicate =
        <dialog_query::attribute::With<note::Title> as Concept>::CONCEPT;

    match concept {
        ConceptPredicate::Static { operator, .. } => {
            println!("✓ Concept operator: {}", operator);
            assert_eq!(operator, "note");
        }
        _ => panic!("Expected Static concept"),
    }
}

#[test]
fn test_attribute_instance_creation() {
    let entity = Entity::new().unwrap();
    let title = note::Title("My Note".to_string());

    // Create an instance using With wrapper
    let instance = dialog_query::attribute::With {
        this: entity.clone(),
        has: title.clone(),
    };

    // Test Instance trait method
    assert_eq!(instance.this(), entity);

    println!("✓ Attribute instance created successfully!");
}

#[test]
fn test_attribute_match_realize() {
    let entity = Entity::new().unwrap();
    let title_value = "Test Title".to_string();

    // Create a match pattern
    let match_pattern = Match::<dialog_query::attribute::With<note::Title>> {
        this: Term::from(entity.clone()),
        has: Term::from(title_value.clone()),
    };

    // Note: Answer creation is complex and requires actual query results
    // This test just verifies the types are correct
    println!("✓ Match pattern created successfully!");
    println!("  Entity: {:?}", match_pattern.this);
    println!("  Title: {:?}", match_pattern.has);
}

#[test]
fn test_attribute_into_iterator() {
    let entity = Entity::new().unwrap();
    let title = note::Title("My Title".to_string());

    let instance = dialog_query::attribute::With {
        this: entity.clone(),
        has: title.clone(),
    };

    // Convert to relations
    let relations: Vec<_> = instance.into_iter().collect();

    assert_eq!(relations.len(), 1);
    println!("✓ With<Attribute> converts to relations!");
    println!("  Relation: {:?}", relations[0]);
}

#[tokio::test]
async fn test_attribute_claim() -> anyhow::Result<()> {
    use dialog_artifacts::{Artifacts, Entity as ArtifactEntity};
    use dialog_query::{claim::Claim, Session, Transaction};
    use dialog_storage::MemoryStorageBackend;

    // Create a store and session
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    let entity = ArtifactEntity::new()?;
    let title = note::Title("Test Note".to_string());

    let instance = dialog_query::attribute::With {
        this: entity.clone(),
        has: title.clone(),
    };

    // Use Claim trait to assert the instance
    let mut transaction = Transaction::new();
    instance.clone().assert(&mut transaction);
    session.commit(transaction).await?;

    // Verify it was stored
    let query = Match::<dialog_query::attribute::With<note::Title>> {
        this: Term::from(entity.clone()),
        has: Term::var("has"),
    };

    let premise: dialog_query::Premise = query.into();
    let application = match premise {
        dialog_query::Premise::Apply(app) => app,
        _ => panic!("Expected Apply premise"),
    };

    use futures_util::TryStreamExt;
    let results = application.query(&session).try_collect::<Vec<_>>().await?;

    assert_eq!(results.len(), 1);
    println!("✓ Attribute Claim trait works!");

    Ok(())
}

#[test]
fn test_attribute_terms() {
    // Access term constructors
    let this_term = dialog_query::attribute::WithTerms::<note::Title>::this();
    let has_term = dialog_query::attribute::WithTerms::<note::Title>::has();

    // These should be variables
    match this_term {
        Term::Variable { name, .. } => assert_eq!(name, Some("this".to_string())),
        _ => panic!("Expected variable"),
    }

    match has_term {
        Term::Variable { name, .. } => assert_eq!(name, Some("has".to_string())),
        _ => panic!("Expected variable"),
    }

    println!("✓ WithTerms work!");
}
