//! Concept derive macro implementation
//!
//! Generates a full ECS-style "concept" from a struct whose fields are `Attribute`
//! types. A concept groups related attributes that together describe an entity.
//!
//! # Example input
//!
//! ```rust,ignore
//! /// A person in the system
//! #[derive(Concept)]
//! pub struct Person {
//!     pub this: Entity,
//!     pub name: FullName,    // FullName: Attribute<Type = String>
//!     pub age: Age,          // Age: Attribute<Type = i64>
//! }
//! ```
//!
//! # Generated output (simplified)
//!
//! ```rust,ignore
//! // -- Compile-time validation --
//! // Asserts that FullName and Age implement Attribute (excludes `this: Entity`)
//!
//! // -- Match struct (query pattern) --
//! // Each attribute field becomes a Term<T> for pattern matching.
//! pub struct PersonMatch {
//!     pub this: Term<Entity>,
//!     pub name: Term<String>,   // Term<FullName::Type>
//!     pub age: Term<i64>,       // Term<Age::Type>
//! }
//! // Default fills every field with a named variable:
//! //   PersonMatch { this: Term::var("this"), name: Term::var("name"), age: Term::var("age") }
//!
//! // -- Terms struct (convenience constructors) --
//! pub struct PersonTerms {}
//! impl PersonTerms {
//!     pub fn this() -> Term<Entity> { Term::var("this") }
//!     pub fn name() -> Term<String> { Term::var("name") }
//!     pub fn age() -> Term<i64> { Term::var("age") }
//! }
//!
//! // -- Attributes struct --
//! // Match<T> wrappers bound to a specific entity term.
//! pub struct PersonAttributes {
//!     pub this: Term<Entity>,
//!     pub name: Match<String>,
//!     pub age: Match<i64>,
//! }
//!
//! // -- Static attribute schemas --
//! // LazyLock statics like PERSON_NAME, PERSON_AGE holding AttributeSchema<T>.
//!
//! // -- Concept trait impl --
//! impl Concept for Person {
//!     type Instance = Person;
//!     type Match = PersonMatch;
//!     type Term = PersonTerms;
//!     const CONCEPT: predicate::concept::Concept = /* static descriptor */;
//! }
//!
//! // -- Match trait impl --
//! // PersonMatch::realize(answer) reconstructs a Person from query results.
//! // PersonMatch::query(source) runs the query and streams Person instances.
//!
//! // -- Instance trait impl --
//! // Person::this() returns the entity.
//!
//! // -- IntoIterator impl --
//! // Iterates over Relation values (one per attribute) for asserting facts.
//!
//! // -- Claim impl --
//! // Person::assert(tx) / Person::retract(tx) for transactional writes.
//!
//! // -- Not impl --
//! // !person syntax for retraction.
//!
//! // -- From impls --
//! // PersonMatch → Parameters, Premise, Application, ConceptApplication
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{extract_doc_comments, to_snake_case};

pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Extract doc comments from the concept struct
    let concept_description = extract_doc_comments(&input.attrs);
    let concept_description_lit =
        syn::LitStr::new(&concept_description, proc_macro2::Span::call_site());

    // Extract fields from the struct
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "Concept can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "Concept can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    // Check for required `this` field.
    // We only check that the field exists — the type is validated at compile time
    // by the generated `Instance` impl which returns `Entity`. If the field's type
    // isn't `Entity`, the user gets a type-mismatch error from the compiler.
    let has_this_field = fields
        .iter()
        .any(|field| field.ident.as_ref().is_some_and(|name| name == "this"));

    if !has_this_field {
        return syn::Error::new_spanned(
            &input,
            "Concept structs must have a `this: Entity` field.\n\
             Add the following field to your struct:\n\
             pub this: Entity\n\n\
             This field is required because every concept instance must be associated with an entity."
        )
        .to_compile_error()
        .into();
    }

    // Collect code fragments for each field. We iterate over fields once and build
    // up parallel vectors of token streams that get spliced into the final output.
    // Each vector holds one fragment per non-`this` field:
    //   match_fields         → Term<T> fields for the Match struct
    //   attributes_fields    → Match<T> fields for the Attributes struct
    //   rule_when_fields     → Fact selectors for the when() rule builder
    //   attribute_init_fields→ field initializers for Attributes::new(entity)
    //   typed_attributes     → LazyLock<AttributeSchema<T>> statics
    //   value_attributes     → AttributeSchema<Value> for the attributes() vec
    //   instance_relations   → Relation constructors for IntoIterator/Claim
    //   terms_methods        → PersonTerms::name() convenience methods
    //   attributes_tuples    → (name, schema) pairs for attribute lookup
    //   match_term_conversions → (unused placeholder for future use)
    let mut match_fields = Vec::new();
    let mut attributes_fields = Vec::new();
    let mut rule_when_fields = Vec::new();
    let mut attribute_init_fields = Vec::new();
    let mut typed_attributes = Vec::new();
    let mut value_attributes = Vec::new();
    let mut field_names = Vec::new();
    let mut field_name_lits = Vec::new();
    let mut field_types = Vec::new();
    let mut match_term_conversions = Vec::new();
    let mut attributes_tuples = Vec::new();
    let mut terms_methods = Vec::new();
    let mut instance_relations = Vec::new();

    // Generate namespace from struct name (e.g., Person -> "person")
    let namespace = to_snake_case(&struct_name.to_string());
    let namespace_lit = syn::LitStr::new(&namespace, proc_macro2::Span::call_site());
    let terms_name = syn::Ident::new(&format!("{}Terms", struct_name), struct_name.span());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();

        // Skip the 'this' field - it's handled specially
        if field_name_str == "this" {
            continue;
        }

        let field_type = &field.ty;
        let field_name_lit = syn::LitStr::new(&field_name_str, proc_macro2::Span::call_site());

        // Store field name and type for later use in reconstruction
        field_names.push(field_name);
        field_types.push(field_type);
        field_name_lits.push(field_name_lit.clone());

        // Extract doc comment from the field (for Match struct docs)
        let doc_comment = extract_doc_comments(&field.attrs);
        let doc_comment_lit = syn::LitStr::new(&doc_comment, proc_macro2::Span::call_site());

        // Extract the inner type from Attribute - the field type implements Attribute
        // and we need <FieldType as Attribute>::Type for the Term wrapper
        let inner_type = quote! { <#field_type as dialog_query::Attribute>::Type };

        // Generate Match field (Term<InnerType>) where InnerType is the Attribute's Type
        match_fields.push(quote! {
            #[doc = #doc_comment_lit]
            pub #field_name: dialog_query::term::Term<#inner_type>
        });

        terms_methods.push(quote! {
            impl #terms_name {
                pub fn #field_name() -> dialog_query::Term<#inner_type> {
                    dialog_query::Term::<#inner_type>::var(#field_name_lit)
                }
            }
        });

        // Generate Attributes field (Match<T>) - T is the Attribute's inner type
        attributes_fields.push(quote! {
            #[doc = #doc_comment_lit]
            pub #field_name: dialog_query::attribute::Match<#inner_type>
        });

        // Generate attribute initialization for Attributes
        let prefixed_field_name = syn::Ident::new(
            &format!(
                "{}_{}",
                namespace.replace(".", "_").to_uppercase(),
                field_name_str.to_uppercase()
            ),
            field_name.span(),
        );
        attribute_init_fields.push(quote! {
            #field_name: #prefixed_field_name.of(entity.clone())
        });

        // Generate static attribute definition by extracting metadata from the Attribute trait
        typed_attributes.push(quote! {
            /// Static attribute definition for #field_name - delegates to Attribute trait
            pub static #prefixed_field_name: std::sync::LazyLock<dialog_query::attribute::AttributeSchema<#inner_type>> =
                std::sync::LazyLock::new(|| dialog_query::attribute::AttributeSchema {
                    namespace: <#field_type as dialog_query::Attribute>::NAMESPACE,
                    name: <#field_type as dialog_query::Attribute>::NAME,
                    description: <#field_type as dialog_query::Attribute>::DESCRIPTION,
                    cardinality: <#field_type as dialog_query::Attribute>::CARDINALITY,
                    content_type: <#inner_type as dialog_query::types::IntoType>::TYPE,
                    marker: std::marker::PhantomData,
                });
        });

        // Generate Attribute<Value> for the attributes() method
        value_attributes.push(quote! {
            dialog_query::attribute::AttributeSchema {
                namespace: <#field_type as dialog_query::Attribute>::NAMESPACE,
                name: <#field_type as dialog_query::Attribute>::NAME,
                description: <#field_type as dialog_query::Attribute>::DESCRIPTION,
                cardinality: <#field_type as dialog_query::Attribute>::CARDINALITY,
                content_type: <#inner_type as dialog_query::types::IntoType>::TYPE,
                marker: std::marker::PhantomData,
            }
        });

        // Generate rule when field conversion
        rule_when_fields.push(quote! {
            {
                let value_term = match &terms.#field_name {
                    dialog_query::term::Term::Variable { name, .. } => dialog_query::term::Term::Variable {
                        name: name.clone(),
                        content_type: Default::default(),
                    },
                    dialog_query::term::Term::Constant(value) => dialog_query::term::Term::Constant(dialog_query::types::Scalar::as_value(value)),
                };

                dialog_query::predicate::fact::Fact::select()
                    .the(<#field_type as dialog_query::Attribute>::selector().to_string())
                    .of(terms.this.clone())
                    .is(value_term)
            }
        });

        // Generate term conversions for Match implementation
        match_term_conversions.push(quote! {
            #field_name_lit => {
                None
            }
        });

        // Generate attribute tuples for Attributes implementation
        attributes_tuples.push(quote! {
            (
                <#field_type as dialog_query::Attribute>::NAME,
                dialog_query::attribute::AttributeSchema {
                    namespace: <#field_type as dialog_query::Attribute>::NAMESPACE,
                    name: <#field_type as dialog_query::Attribute>::NAME,
                    description: <#field_type as dialog_query::Attribute>::DESCRIPTION,
                    cardinality: <#field_type as dialog_query::Attribute>::CARDINALITY,
                    content_type: <#inner_type as dialog_query::types::IntoType>::TYPE,
                    marker: std::marker::PhantomData,
                }
            )
        });

        // Generate Relation for IntoIterator implementation
        instance_relations.push(quote! {
            dialog_query::Relation::new(
                <#field_type as dialog_query::Attribute>::selector(),
                self.this.clone(),
                dialog_query::types::Scalar::as_value(<#field_type as dialog_query::Attribute>::value(&self.#field_name)),
            )
        });
    }

    // Generate type names based on struct name
    let match_name = syn::Ident::new(&format!("{}Match", struct_name), struct_name.span());
    let attributes_name =
        syn::Ident::new(&format!("{}Attributes", struct_name), struct_name.span());

    // Generate static array names
    let attributes_array_name = syn::Ident::new(
        &format!(
            "{}_ATTRIBUTES_ARRAY",
            struct_name.to_string().to_uppercase()
        ),
        struct_name.span(),
    );
    let attribute_tuples_name = syn::Ident::new(
        &format!(
            "{}_ATTRIBUTE_TUPLES",
            struct_name.to_string().to_uppercase()
        ),
        struct_name.span(),
    );
    let attributes_const_name = syn::Ident::new(
        &format!("{}_ATTRIBUTES", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let operator_const_name = syn::Ident::new(
        &format!("{}_OPERATOR", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Create validation function name
    let validate_fn_name = syn::Ident::new(
        &format!("validate_{}", struct_name.to_string().to_lowercase()),
        struct_name.span(),
    );

    let expanded = quote! {
        // Compile-time validation that all fields (except 'this') implement Attribute
        const _: () = {
            fn assert_implements_attribute<T: dialog_query::Attribute>() {}
            fn #validate_fn_name() {
                #(assert_implements_attribute::<#field_types>();)*
            }
        };

        /// Match pattern for #struct_name - has Term-wrapped fields for querying
        #[derive(Debug, Clone, PartialEq)]
        pub struct #match_name {
            /// The entity being matched
            pub this: dialog_query::term::Term<dialog_query::artifact::Entity>,
            #(#match_fields),*
        }

        impl Default for #match_name {
            fn default() -> Self {
                Self {
                    this: dialog_query::Term::var("this"),
                    #(#field_names: dialog_query::Term::var(#field_name_lits)),*
                }
            }
        }

        #[derive(Debug, Clone, PartialEq)]
        pub struct #terms_name {}
        impl #terms_name {
            pub fn this() -> dialog_query::Term<dialog_query::Entity> {
                dialog_query::Term::<dialog_query::Entity>::var("this")
            }
        }
        #(#terms_methods)*

        /// Attributes pattern for #struct_name - enables fluent query building
        #[derive(Debug, Clone, PartialEq)]
        pub struct #attributes_name {
            pub this: dialog_query::term::Term<dialog_query::artifact::Entity>,
            #(#attributes_fields),*
        }

        // Static attribute definitions
        #(#typed_attributes)*

        /// All attributes as Attribute<Value> for the attributes() method
        pub static #attributes_array_name: std::sync::LazyLock<Vec<dialog_query::attribute::AttributeSchema<dialog_query::artifact::Value>>> =
            std::sync::LazyLock::new(|| vec![
                #(#value_attributes),*
            ]);

        /// Attribute tuples for the Attributes trait implementation
        pub static #attribute_tuples_name: std::sync::LazyLock<Vec<(String, dialog_query::attribute::AttributeSchema<dialog_query::artifact::Value>)>> =
            std::sync::LazyLock::new(|| vec![
                #(
                    (#field_name_lits.to_string(), dialog_query::attribute::AttributeSchema {
                        namespace: <#field_types as dialog_query::Attribute>::NAMESPACE,
                        name: <#field_types as dialog_query::Attribute>::NAME,
                        description: <#field_types as dialog_query::Attribute>::DESCRIPTION,
                        cardinality: <#field_types as dialog_query::Attribute>::CARDINALITY,
                        content_type: <<#field_types as dialog_query::Attribute>::Type as dialog_query::types::IntoType>::TYPE,
                        marker: std::marker::PhantomData,
                    })
                ),*
            ]);

        /// Static const array of attribute tuples for CONCEPT
        static #attributes_const_name: &[(&str, dialog_query::attribute::AttributeSchema<dialog_query::artifact::Value>)] = &[
            #(
                (#field_name_lits, dialog_query::attribute::AttributeSchema {
                    namespace: <#field_types as dialog_query::Attribute>::NAMESPACE,
                    name: <#field_types as dialog_query::Attribute>::NAME,
                    description: <#field_types as dialog_query::Attribute>::DESCRIPTION,
                    cardinality: <#field_types as dialog_query::Attribute>::CARDINALITY,
                    content_type: <<#field_types as dialog_query::Attribute>::Type as dialog_query::types::IntoType>::TYPE,
                    marker: std::marker::PhantomData,
                })
            ),*
        ];

        /// Const operator name for this concept
        pub const #operator_const_name: &str = #namespace_lit;

        // Implement Match trait for the Match struct
        impl dialog_query::concept::Match for #match_name {
            type Concept = #struct_name;
            type Instance = #struct_name;

            fn realize(&self, source: dialog_query::selection::Answer) -> std::result::Result<Self::Instance, dialog_query::QueryError> {
                Ok(#struct_name {
                    this: source.get(&self.this)?,
                    #(#field_names: #field_types(source.get(&self.#field_names)?)),*
                })
            }
        }

        // Add inherent query method so users don't need to import Query trait
        impl #match_name {
            /// Query for instances matching this pattern
            pub fn query<S: dialog_query::query::Source>(
                &self,
                source: S,
            ) -> impl dialog_query::query::Output<#struct_name> {
                use futures_util::StreamExt;
                let application: dialog_query::application::concept::ConceptApplication = self.clone().into();
                let cloned = self.clone();
                application
                    .query(source)
                    .map(move |input| dialog_query::concept::Match::realize(&cloned, input?))
            }
        }

        // Implement From<Match> for Parameters to satisfy Into<Parameters> bound
        impl From<#match_name> for dialog_query::Parameters {
            fn from(source: #match_name) -> Self {
                let mut terms = Self::new();

                terms.insert("this".into(), source.this.as_unknown());

                #(terms.insert(#field_name_lits.into(), source.#field_names.as_unknown());)*

                terms
            }
        }

        // Implement From<Match> for Premise
        impl From<#match_name> for dialog_query::Premise {
            fn from(source: #match_name) -> Self {
                let app = dialog_query::application::concept::ConceptApplication {
                    terms: source.into(),
                    concept: #struct_name::CONCEPT,
                };
                dialog_query::Premise::Apply(dialog_query::Application::Concept(app))
            }
        }

        // Implement From<Match> for Application
        impl From<#match_name> for dialog_query::Application {
            fn from(source: #match_name) -> Self {
                let app = dialog_query::application::concept::ConceptApplication {
                    terms: source.into(),
                    concept: #struct_name::CONCEPT,
                };
                dialog_query::Application::Concept(app)
            }
        }

        // Implement From<Match> for ConceptApplication
        impl From<#match_name> for dialog_query::application::concept::ConceptApplication {
            fn from(source: #match_name) -> Self {
                dialog_query::application::concept::ConceptApplication {
                    terms: source.into(),
                    concept: #struct_name::CONCEPT,
                }
            }
        }

        // Implement From<&Match> for ConceptApplication
        impl From<&#match_name> for dialog_query::application::concept::ConceptApplication {
            fn from(source: &#match_name) -> Self {
                dialog_query::application::concept::ConceptApplication {
                    terms: source.into(),
                    concept: #struct_name::CONCEPT,
                }
            }
        }

        // Implement Instance trait for the concept struct
        impl dialog_query::concept::Instance for #struct_name {
            fn this(&self) -> &dialog_query::artifact::Entity {
                &self.this
            }
        }

        // Implement Concept trait
        impl dialog_query::concept::Concept for #struct_name {
            type Instance = #struct_name;
            type Match = #match_name;
            type Term = #terms_name;

            const CONCEPT: dialog_query::predicate::concept::Concept =
                dialog_query::predicate::concept::Concept::Static {
                    description: #concept_description_lit,
                    attributes: &dialog_query::predicate::concept::Attributes::Static(#attributes_const_name),
                };
        }

        // Implement IntoIterator to convert concept into relations
        impl IntoIterator for #struct_name {
            type Item = dialog_query::Relation;
            type IntoIter = std::vec::IntoIter<dialog_query::Relation>;

            fn into_iter(self) -> Self::IntoIter {
                vec![
                    #(#instance_relations),*
                ].into_iter()
            }
        }

        // Implement Claim trait
        impl dialog_query::claim::Claim for #struct_name {
            fn assert(self, transaction: &mut dialog_query::Transaction) {
                #(
                    #instance_relations.assert(transaction);
                )*
            }

            fn retract(self, transaction: &mut dialog_query::Transaction) {
                #(
                    #instance_relations.retract(transaction);
                )*
            }
        }

        // Implement Not trait to enable !concept syntax for retraction
        impl std::ops::Not for #struct_name {
            type Output = dialog_query::claim::Revert<Self>;

            fn not(self) -> Self::Output {
                dialog_query::claim::Claim::revert(self)
            }
        }

        impl dialog_query::dsl::Quarriable for #struct_name {
            type Query = #match_name;
        }

        // Implement Rule trait
        impl #struct_name {
            fn when(terms: dialog_query::Match<Self>) -> dialog_query::rule::Premises {
                let selectors = vec![
                    #(#rule_when_fields),*
                ];

                selectors.into()
            }
        }
    };

    TokenStream::from(expanded)
}
