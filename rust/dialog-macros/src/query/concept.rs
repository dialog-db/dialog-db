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
//! // -- Concept trait impl --
//! impl Concept for Person {
//!     type Conclusion = Person;
//!     type Query = PersonMatch;
//!     type Term = PersonTerms;
//!     fn predicate() -> predicate::concept::ConceptDescriptor { /* construct predicate */ }
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
//! // -- Statement impl --
//! // Person::assert(tx) / Person::retract(tx) for transactional writes.
//!
//! // -- Not impl --
//! // !person syntax for retraction.
//!
//! // -- From impls --
//! // PersonMatch → Parameters, Premise, Application, ConceptQuery
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
    let mut match_fields = Vec::new();
    let mut rule_when_fields = Vec::new();
    let mut field_names = Vec::new();
    let mut field_name_lits = Vec::new();
    let mut field_types = Vec::new();
    let mut terms_methods = Vec::new();
    let mut instance_relations = Vec::new();

    // Generate domain from struct name (e.g., Person -> "person")
    let domain = to_snake_case(&struct_name.to_string());
    let domain_lit = syn::LitStr::new(&domain, proc_macro2::Span::call_site());
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
            pub #field_name: dialog_query::Term<#inner_type>
        });

        terms_methods.push(quote! {
            impl #terms_name {
                pub fn #field_name() -> dialog_query::Term<#inner_type> {
                    dialog_query::Term::<#inner_type>::var(#field_name_lit)
                }
            }
        });

        // Generate rule when field conversion
        rule_when_fields.push(quote! {
            {
                let value_param = dialog_query::Term::<dialog_query::types::Any>::from(terms.#field_name.clone());

                dialog_query::RelationQuery::new(
                    dialog_query::Term::Constant(dialog_query::Value::from(<#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().the().clone())),
                    terms.this.clone(),
                    value_param,
                    dialog_query::Term::blank(),
                    Some(<#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().cardinality()),
                )
            }
        });

        // Generate Association for IntoIterator implementation
        instance_relations.push(quote! {
            dialog_query::Association::new(
                <#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().the().clone(),
                self.this.clone(),
                dialog_query::Value::from(<#field_type as dialog_query::Attribute>::value(&self.#field_name).clone()),
            )
        });
    }

    // Generate type names based on struct name
    let match_name = syn::Ident::new(&format!("{}Match", struct_name), struct_name.span());

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
        // Compile-time validation that all fields (except 'this') implement Attribute + Descriptor
        const _: () = {
            fn assert_implements_attribute<T: dialog_query::Attribute + dialog_query::Descriptor<dialog_query::AttributeDescriptor>>() {}
            fn #validate_fn_name() {
                #(assert_implements_attribute::<#field_types>();)*
            }
        };

        /// Match pattern for #struct_name - has Term-wrapped fields for querying
        #[derive(Debug, Clone, PartialEq)]
        pub struct #match_name {
            /// The entity being matched
            pub this: dialog_query::Term<dialog_query::Entity>,
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

        /// Const operator name for this concept
        pub const #operator_const_name: &str = #domain_lit;

        // Implement Queryable trait for the Match struct
        impl dialog_query::Application for #match_name {
            type Conclusion = #struct_name;

            fn evaluate<S: dialog_query::Source, M: dialog_query::Answers>(
                self,
                answers: M,
                source: &S,
            ) -> impl dialog_query::Answers {
                let application: dialog_query::ConceptQuery = self.into();
                application.evaluate(answers, source)
            }

            fn realize(&self, source: dialog_query::Answer) -> std::result::Result<Self::Conclusion, dialog_query::EvaluationError> {
                Ok(#struct_name {
                    this: source.get(&self.this)?,
                    #(#field_names: #field_types(source.get(&self.#field_names)?)),*
                })
            }
        }

        // Add inherent perform method so users don't need to import Application trait
        impl #match_name {
            /// Execute this query against the given source
            pub fn perform<S: dialog_query::Source>(
                self,
                source: &S,
            ) -> impl dialog_query::Output<#struct_name> {
                dialog_query::Application::perform(self, source)
            }
        }

        // Implement From<Match> for Parameters to satisfy Into<Parameters> bound
        impl From<#match_name> for dialog_query::Parameters {
            fn from(source: #match_name) -> Self {
                let mut terms = Self::new();

                terms.insert("this".into(), dialog_query::Term::<dialog_query::types::Any>::from(source.this));

                #(terms.insert(#field_name_lits.into(), dialog_query::Term::<dialog_query::types::Any>::from(source.#field_names));)*

                terms
            }
        }

        // Implement From<StructName> for ConceptDescriptor
        impl From<#struct_name> for dialog_query::ConceptDescriptor {
            fn from(_: #struct_name) -> Self {
                dialog_query::ConceptDescriptor::from(vec![
                    #(
                        (#field_name_lits, <#field_types as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().clone())
                    ),*
                ])
            }
        }

        // Implement From<MatchName> for ConceptDescriptor
        impl From<#match_name> for dialog_query::ConceptDescriptor {
            fn from(_: #match_name) -> Self {
                dialog_query::ConceptDescriptor::from(vec![
                    #(
                        (#field_name_lits, <#field_types as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().clone())
                    ),*
                ])
            }
        }

        // Implement From<Match> for Premise
        impl From<#match_name> for dialog_query::Premise {
            fn from(source: #match_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                };
                dialog_query::Premise::Assert(dialog_query::Proposition::Concept(app))
            }
        }

        // Implement From<Match> for Application
        impl From<#match_name> for dialog_query::Proposition {
            fn from(source: #match_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                };
                dialog_query::Proposition::Concept(app)
            }
        }

        // Implement From<Match> for ConceptQuery
        impl From<#match_name> for dialog_query::ConceptQuery {
            fn from(source: #match_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                }
            }
        }

        // Implement From<&Match> for ConceptQuery
        impl From<&#match_name> for dialog_query::ConceptQuery {
            fn from(source: &#match_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                }
            }
        }

        // Implement Instance trait for the concept struct
        impl dialog_query::Conclusion for #struct_name {
            fn this(&self) -> &dialog_query::Entity {
                &self.this
            }
        }

        // Implement Concept trait
        impl dialog_query::Concept for #struct_name {
            type Term = #terms_name;

            fn description() -> &'static str {
                #concept_description_lit
            }

            fn this(&self) -> dialog_artifacts::Entity {
                let predicate: dialog_query::ConceptDescriptor = self.clone().into();
                predicate.this()
            }
        }

        // Implement IntoIterator to convert concept into relations
        impl IntoIterator for #struct_name {
            type Item = dialog_query::Association;
            type IntoIter = std::vec::IntoIter<dialog_query::Association>;

            fn into_iter(self) -> Self::IntoIter {
                vec![
                    #(#instance_relations),*
                ].into_iter()
            }
        }

        // Implement Statement trait
        impl dialog_query::Statement for #struct_name {
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
            type Output = dialog_query::Retraction<Self>;

            fn not(self) -> Self::Output {
                dialog_query::Statement::revert(self)
            }
        }

        impl dialog_query::Predicate for #struct_name {
            type Conclusion = #struct_name;
            type Application = #match_name;
            type Descriptor = dialog_query::ConceptDescriptor;
        }

        // Implement Rule trait
        impl #struct_name {
            fn when(terms: dialog_query::Query<Self>) -> dialog_query::Premises {
                let selectors = vec![
                    #(#rule_when_fields),*
                ];

                selectors.into()
            }
        }
    };

    TokenStream::from(expanded)
}
