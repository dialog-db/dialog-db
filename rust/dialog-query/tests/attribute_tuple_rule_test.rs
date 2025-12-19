//! Test Match::<(Entity, Attribute)> syntax for use in rule definitions

use dialog_query::rule::When;
use dialog_query::{Concept, Entity, Match, Term};

mod note {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Title(pub String);
}

mod note_v2 {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct Note {
    pub this: Entity,
    pub title: note::Title,
}

// This is the rule function that migrates from note::Title to note_v2::Name
pub fn v2(terms: Match<Note>) -> impl When {
    (
        Match::<dialog_query::attribute::With<note_v2::Name>> {
            this: terms.this.clone(),
            has: terms.title.clone(),
        },
        !Match::<dialog_query::attribute::With<note::Title>> {
            this: terms.this,
            has: terms.title,
        },
    )
}

#[test]
fn test_match_tuple_syntax_in_rule() {
    // Create a match pattern
    let note_match = Match::<Note> {
        this: Term::var("note"),
        title: Term::var("title"),
    };

    // Call the rule function
    let premises = v2(note_match);

    // Convert to premises
    let premises_vec = premises.into_premises().into_vec();

    // Should have 2 premises: one assertion, one negation
    assert_eq!(premises_vec.len(), 2);

    println!("✓ Rule premises generated successfully using Match::<(Entity, A)> syntax!");
    println!("  Premise 1 (assert note_v2::Name): {:?}", premises_vec[0]);
    println!("  Premise 2 (retract note::Title): {:?}", premises_vec[1]);
}

#[test]
fn test_match_tuple_type_resolution() {
    // Test that Match::<With<Attribute>> resolves to the right type
    let entity_term = Term::var("entity");
    let title_term = Term::var("title");

    // Match::<With<note::Title>> with struct literal syntax
    let _assertion = Match::<dialog_query::attribute::With<note::Title>> {
        this: entity_term.clone(),
        has: title_term.clone(),
    };

    // Test negation syntax
    let _negation = !Match::<dialog_query::attribute::With<note::Title>> {
        this: entity_term,
        has: title_term,
    };

    println!("✓ Match::<With<Attribute>> type resolution works!");
}

#[test]
fn test_match_with_constants() {
    // Test using constants in Match
    let entity = Entity::new().unwrap();
    let title = "My Note".to_string();

    let assertion = Match::<dialog_query::attribute::With<note::Title>> {
        this: Term::from(entity),
        has: Term::from(title),
    };

    // Convert to premise
    let premise: dialog_query::Premise = assertion.into();

    println!("✓ Match with constants works!");
    println!("  Premise: {:?}", premise);
}

#[tokio::test]
async fn test_adhoc_concept_query() -> anyhow::Result<()> {
    use dialog_artifacts::{Artifacts, Attribute, Entity as ArtifactEntity};
    use dialog_query::{Relation, Session, Term, Value};
    use dialog_storage::MemoryStorageBackend;

    // Create a store and session
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    // Create some test entities with titles
    let note1 = ArtifactEntity::new()?;
    let note2 = ArtifactEntity::new()?;
    let note3 = ArtifactEntity::new()?;

    // Add titles to the entities
    session
        .transact(vec![
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note1.clone(),
                is: Value::String("First Note".to_string()),
            },
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note2.clone(),
                is: Value::String("Second Note".to_string()),
            },
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note3.clone(),
                is: Value::String("Third Note".to_string()),
            },
        ])
        .await?;

    // Query for all notes with titles using Match syntax
    let query = Match::<dialog_query::attribute::With<note::Title>> {
        this: Term::var("entity"),
        has: Term::var("has"),
    };

    // Convert to premise and extract the application
    let premise: dialog_query::Premise = query.into();

    // Extract the application from the premise
    let application = match premise {
        dialog_query::Premise::Apply(app) => app,
        _ => panic!("Expected Apply premise"),
    };

    // Execute the query
    use futures_util::TryStreamExt;
    let results = application.query(&session).try_collect::<Vec<_>>().await?;

    // Should find all 3 notes
    assert_eq!(results.len(), 3, "Should find 3 notes with titles");

    // Verify we can access the results
    let title_var: Term<Value> = Term::var("has");

    let mut found_titles = std::collections::HashSet::new();
    for result in &results {
        let title = result.resolve(&title_var)?;
        if let Value::String(s) = title {
            found_titles.insert(s.clone());
        }
    }

    assert!(found_titles.contains("First Note"));
    assert!(found_titles.contains("Second Note"));
    assert!(found_titles.contains("Third Note"));

    println!("✓ Ad-hoc concept query works!");
    println!("  Found {} notes with titles", results.len());

    Ok(())
}

#[tokio::test]
async fn test_adhoc_concept_query_with_filter() -> anyhow::Result<()> {
    use dialog_artifacts::{Artifacts, Attribute, Entity as ArtifactEntity};
    use dialog_query::{Relation, Session, Term, Value};
    use dialog_storage::MemoryStorageBackend;

    // Create a store and session
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    // Create some test entities
    let note1 = ArtifactEntity::new()?;
    let note2 = ArtifactEntity::new()?;
    let note3 = ArtifactEntity::new()?;

    session
        .transact(vec![
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note1.clone(),
                is: Value::String("Target Note".to_string()),
            },
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note2.clone(),
                is: Value::String("Other Note".to_string()),
            },
            Relation {
                the: "note/title".parse::<Attribute>()?,
                of: note3.clone(),
                is: Value::String("Another Note".to_string()),
            },
        ])
        .await?;

    // Query for notes with specific title using Match syntax - use constant for filtering
    let query = Match::<dialog_query::attribute::With<note::Title>> {
        this: Term::var("entity"),
        has: Term::from("Target Note".to_string()),
    };

    let premise: dialog_query::Premise = query.into();

    // Extract the application from the premise
    let application = match premise {
        dialog_query::Premise::Apply(app) => app,
        _ => panic!("Expected Apply premise"),
    };

    use futures_util::TryStreamExt;
    let results = application.query(&session).try_collect::<Vec<_>>().await?;

    // Should find only 1 note
    assert_eq!(
        results.len(),
        1,
        "Should find exactly 1 note with 'Target Note' title"
    );

    // Verify it's the correct entity
    let entity_var: Term<Value> = Term::var("entity");
    let found_entity = results[0].resolve(&entity_var)?;
    assert_eq!(found_entity, Value::Entity(note1));

    println!("✓ Ad-hoc concept query with filter works!");
    println!("  Found the correct entity");

    Ok(())
}
