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
//! // -- Query struct (query pattern) --
//! // Each attribute field becomes a Term<T> for pattern matching.
//! pub struct PersonQuery {
//!     pub this: Term<Entity>,
//!     pub name: Term<String>,   // Term<FullName::Type>
//!     pub age: Term<i64>,       // Term<Age::Type>
//! }
//! // Default fills every field with a named variable:
//! // PersonQuery {
//! //      this: Term::var("this"),
//! //      name: Term::var("name"),
//! //      age: Term::var("age")
//! // }
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
//!     type Query = PersonQuery;
//!     type Term = PersonTerms;
//!     fn predicate() -> predicate::concept::ConceptDescriptor { /* construct predicate */ }
//! }
//!
//! // -- Application trait impl --
//! // PersonQuery::realize(candidate) reconstructs a Person from query results.
//! // PersonQuery::perform(source) runs the query and streams Person instances.
//!
//! // -- Instance trait impl --
//! // Person::this() returns the entity.
//!
//! // -- IntoIterator impl --
//! // Iterates over AttributeStatement values (one per attribute) for asserting facts.
//!
//! // -- Statement impl --
//! // Person::assert(tx) / Person::retract(tx) for transactional writes.
//!
//! // -- Not impl --
//! // !person syntax for retraction.
//!
//! // -- From impls --
//! // PersonQuery → Parameters, Premise, Proposition, ConceptQuery
//! ```

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{extract_doc_comments, parse_dialog_field_attributes};

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
    // We only check that the field exists; the type is validated at compile time
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

    // Collect code fragments for each field. The Concept derive
    // does **not** branch syntactically on `Option<T>`. Instead it
    // emits trait-based code that delegates to the `ConceptField`
    // trait, which has two non-overlapping blanket impls in
    // dialog-query:
    //
    // - `impl<N: Attribute> ConceptField for N` (required path)
    // - `impl<N: Attribute> ConceptField for Option<N>` (optional)
    //
    // Rust's coherence permits these because `Option` is
    // `#[fundamental]`. The macro emits `<F as ConceptField>::*`
    // and the type system picks the right impl at type-check time,
    // so aliases, prelude paths, and renamed imports all work
    // without macro-time syntactic detection.
    let mut query_fields = Vec::new();
    let mut field_names = Vec::new();
    let mut field_name_lits = Vec::new();
    let mut field_types = Vec::new();
    let mut realize_fields = Vec::new();
    let mut param_inserts = Vec::new();
    let mut terms_methods = Vec::new();
    let mut descriptor_pair_pushes = Vec::new();
    let mut statement_emits = Vec::new();

    let terms_name = syn::Ident::new(&format!("{}Terms", struct_name), struct_name.span());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        // The Rust field name is normalized for the descriptor / query
        // surface: `unraw()` drops any `r#` raw-identifier prefix, then
        // `to_case(Case::Kebab)` matches the formal-notation convention
        // used by attribute names elsewhere.
        let raw_field_name = field_name.unraw().to_string();
        let field_name_str = raw_field_name.to_case(Case::Kebab);

        // Skip the 'this' field - it's handled specially
        if raw_field_name == "this" {
            continue;
        }

        let field_type = &field.ty;

        // Field-level #[dialog(...)] parameters: rename overrides
        // the string key; conforms marks a concept-typed field.
        let dialog_attrs = match parse_dialog_field_attributes(&field.attrs) {
            Ok(parsed) => parsed,
            Err(e) => return e.to_compile_error().into(),
        };
        let effective_name_str = dialog_attrs
            .rename
            .unwrap_or_else(|| field_name_str.clone());
        let field_name_lit = syn::LitStr::new(&effective_name_str, proc_macro2::Span::call_site());

        // Forward the user's field doc when present, otherwise synthesize a
        // fallback so `#![deny(missing_docs)]` in consumer crates is happy.
        let user_doc = extract_doc_comments(&field.attrs);
        let query_field_doc = if user_doc.is_empty() {
            format!("Term matching the `{field_name_str}` field of [`{struct_name}`].",)
        } else {
            user_doc
        };
        let query_field_doc_lit =
            syn::LitStr::new(&query_field_doc, proc_macro2::Span::call_site());
        let terms_method_doc =
            format!("Variable term for the `{field_name_str}` field of [`{struct_name}`].",);
        let terms_method_doc_lit =
            syn::LitStr::new(&terms_method_doc, proc_macro2::Span::call_site());

        // The query field's term type is driven by ConceptField:
        // - Required field `N`: `<N as ConceptField>::TermType` =
        //   `<N as Attribute>::Type`.
        // - Optional field `Option<N>`: `<Option<N> as ConceptField>::TermType`
        //   = `Option<<N as Attribute>::Type>`.
        let term_type = quote! {
            dialog_query::Term<<#field_type as dialog_query::ConceptField>::TermType>
        };

        query_fields.push(quote! {
            #[doc = #query_field_doc_lit]
            pub #field_name: #term_type
        });

        terms_methods.push(quote! {
            #[doc = #terms_method_doc_lit]
            pub fn #field_name() -> #term_type {
                <#term_type>::var(#field_name_lit)
            }
        });

        // Realize via the trait method. Required and optional impls
        // each handle their own Binding semantics.
        realize_fields.push(quote! {
            #field_name: <#field_type as dialog_query::ConceptField>::realize(
                source.lookup(&dialog_query::Term::<dialog_query::types::Any>::from(&self.#field_name))?
            )?
        });

        param_inserts.push(quote! {
            terms.insert(
                #field_name_lit.into(),
                dialog_query::Term::<dialog_query::types::Any>::from(source.#field_name),
            );
        });

        // Each field becomes a `ConceptFieldDescriptor` via
        // `ConceptField::field_descriptor`, which wraps the
        // attribute's descriptor with this field's optionality
        // (tagged from the `OPTIONAL` const, never by syntactic
        // `Option<_>` inspection).
        //
        // A `#[dialog(conforms = C)]` field goes through
        // `ConformingField::conforming_descriptor` instead, which
        // tags the descriptor with the target concept. The trait is
        // implemented only for required, entity-valued attribute
        // newtypes, so a non-entity or `Option<_>` field fails to
        // compile rather than producing an invalid descriptor.
        match &dialog_attrs.conforms {
            Some(target) => descriptor_pair_pushes.push(quote! {
                __fields.push((
                    #field_name_lit.to_string(),
                    <#field_type as dialog_query::ConformingField>::conforming_descriptor(
                        <#target as dialog_query::Descriptor<
                            dialog_query::ConceptDescriptor,
                        >>::descriptor()
                        .clone(),
                    ),
                ));
            }),
            None => descriptor_pair_pushes.push(quote! {
                __fields.push((
                    #field_name_lit.to_string(),
                    <#field_type as dialog_query::ConceptField>::field_descriptor(),
                ));
            }),
        }

        // Statement emission via the trait method. The concept's
        // `this` field is an `Entity` (concept structs always have
        // `this: Entity`), passed through to each field's
        // statement(s). Required impls push one statement; optional
        // impls push zero or one depending on Some/None.
        statement_emits.push(quote! {
            <#field_type as dialog_query::ConceptField>::push_statements(
                &self.#field_name,
                self.this.clone(),
                &mut __statements,
            );
        });

        field_names.push(field_name);
        field_name_lits.push(field_name_lit);
        field_types.push(field_type);
    }

    // Compile-time assertion list: every field type must implement
    // ConceptField. The trait's blanket impls make this true for
    // any `N: Attribute + Descriptor<AttributeDescriptor>` and for
    // any `Option<N>` with the same bound on `N`.
    let validated_types: Vec<_> = field_types.iter().map(|t| quote! { #t }).collect();

    // Generate type names based on struct name
    let query_name = syn::Ident::new(&format!("{}Query", struct_name), struct_name.span());

    // Create validation function name
    let validate_fn_name = syn::Ident::new(
        &format!("validate_{}", struct_name.to_string().to_lowercase()),
        struct_name.span(),
    );

    // Synthesized docs for generated types so consumer crates can enable
    // `#![deny(missing_docs)]` without the lint firing on code they did
    // not author. We only document items we fabricate; the user's own
    // struct is left untouched so rustc can still warn if they forgot
    // docs on their own concept.
    let query_struct_doc = syn::LitStr::new(
        &format!(
            "Query pattern for [`{struct_name}`]. Each field is a [`dialog_query::Term`] used to match or bind that field when running the concept query.",
        ),
        proc_macro2::Span::call_site(),
    );
    let terms_struct_doc = syn::LitStr::new(
        &format!(
            "Typed variable-term accessors for [`{struct_name}`]. Each associated function returns a named `Term` variable for the corresponding concept field.",
        ),
        proc_macro2::Span::call_site(),
    );
    let terms_this_doc = syn::LitStr::new(
        &format!("Variable term for the `this` field of [`{struct_name}`]."),
        proc_macro2::Span::call_site(),
    );
    let query_this_field_doc = syn::LitStr::new(
        &format!("Term matching the `this` entity of [`{struct_name}`]."),
        proc_macro2::Span::call_site(),
    );

    let expanded = quote! {
        // Compile-time validation that every concept field
        // implements [`ConceptField`](dialog_query::ConceptField).
        // The trait's two blanket impls cover both required
        // `T: Attribute` and optional `Option<T>` shapes; anything
        // outside that pair (e.g. `String`, a non-attribute newtype,
        // `Option<Option<T>>`) fails this assertion with a clear
        // bound-not-satisfied error.
        const _: () = {
            fn assert_implements_concept_field<F: dialog_query::ConceptField>() {}
            fn #validate_fn_name() {
                #(assert_implements_concept_field::<#validated_types>();)*
            }
        };

        // Compile-time validation that the concept declares at least
        // one *required* attribute. A concept made only of optional
        // (`Option<_>`) fields, or of no attribute fields at all,
        // constrains nothing, so every entity would match it; that
        // is rejected here.
        //
        // The required/optional split is read from the
        // [`ConceptField::OPTIONAL`](dialog_query::ConceptField)
        // associated const (a compile-time `bool` chosen by trait
        // dispatch), never by syntactic inspection of `Option<_>`.
        // Summing `!OPTIONAL` over the fields and asserting the total
        // is non-zero gives a `const` check that fires at compile
        // time with a clear message.
        const _: () = {
            let required_field_count: usize = 0
                #( + (!<#field_types as dialog_query::ConceptField>::OPTIONAL as usize) )*;
            assert!(
                required_field_count >= 1,
                "a Concept must declare at least one required (non-Option) attribute field; \
                 a concept built only from optional fields constrains nothing and matches every entity"
            );
        };

        #[doc = #query_struct_doc]
        #[derive(Debug, Clone, PartialEq)]
        pub struct #query_name {
            #[doc = #query_this_field_doc]
            pub this: dialog_query::Term<dialog_query::Entity>,
            #(#query_fields,)*
        }

        impl Default for #query_name {
            fn default() -> Self {
                Self {
                    this: dialog_query::Term::var("this"),
                    #(#field_names: dialog_query::Term::var(#field_name_lits),)*
                }
            }
        }

        #[doc = #terms_struct_doc]
        #[derive(Debug, Clone, PartialEq)]
        pub struct #terms_name {}
        impl #terms_name {
            #[doc = #terms_this_doc]
            pub fn this() -> dialog_query::Term<dialog_query::Entity> {
                dialog_query::Term::<dialog_query::Entity>::var("this")
            }
            #(#terms_methods)*
        }

        // Implement Application trait for the Query struct
        impl dialog_query::Application for #query_name {
            type Conclusion = #struct_name;

            fn evaluate<'__a, __Env, __M: dialog_query::Selection + '__a>(
                self,
                selection: __M,
                env: &'__a __Env,
            ) -> impl dialog_query::Selection + '__a
            where
                __Env: dialog_query::Provider<dialog_query::Select<'__a>>
                    + dialog_query::Provider<dialog_query::source::SelectRules>
                    + dialog_query::ConditionalSync,
            {
                let application: dialog_query::ConceptQuery = self.into();
                application.evaluate(selection, env)
            }

            fn realize(&self, source: dialog_query::Match) -> std::result::Result<Self::Conclusion, dialog_query::EvaluationError> {
                Ok(#struct_name {
                    this: dialog_query::Entity::try_from(
                        source.lookup(&dialog_query::Term::from(&self.this))?.content()?
                    )?,
                    #(#realize_fields,)*
                })
            }
        }

        // Add inherent perform method so users don't need to import Application trait
        impl #query_name {
            /// Execute this query against the given environment
            pub fn perform<'__a, __Env>(
                self,
                env: &'__a __Env,
            ) -> impl dialog_query::Output<#struct_name> + '__a
            where
                __Env: dialog_query::Provider<dialog_query::Select<'__a>>
                    + dialog_query::Provider<dialog_query::source::SelectRules>
                    + dialog_query::ConditionalSync,
            {
                dialog_query::Application::perform(self, env)
            }
        }

        // Implement From<Query> for Parameters
        impl From<#query_name> for dialog_query::Parameters {
            fn from(source: #query_name) -> Self {
                let mut terms = Self::new();

                terms.insert("this".into(), dialog_query::Term::<dialog_query::types::Any>::from(source.this));

                #(#param_inserts)*

                terms
            }
        }

        // The concept's runtime schema (`ConceptDescriptor`), cached.
        // This is the single construction site for the descriptor;
        // the `From<#query_name>` conversions below delegate here.
        //
        // Each field becomes a `ConceptFieldDescriptor` via
        // `ConceptField::field_descriptor`, which carries the field's
        // optionality (from the `OPTIONAL` const). All fields live in
        // the descriptor's single `with` map; optionality is a
        // per-field flag. The struct's doc comment carries through as
        // the descriptor's `description` so a `concept:` query
        // surfaces it (the field list alone leaves it `None`).
        //
        // Building goes through the fallible
        // `ConceptDescriptor::try_from`, which only rejects a set
        // with no required field; ruled out at compile time by the
        // required-field assertion above, so the `expect` is
        // statically unreachable (it documents the invariant rather
        // than handling a real failure).
        impl dialog_query::Descriptor<dialog_query::ConceptDescriptor> for #struct_name {
            fn descriptor() -> &'static dialog_query::ConceptDescriptor {
                static DESCRIPTOR: std::sync::OnceLock<dialog_query::ConceptDescriptor> =
                    std::sync::OnceLock::new();
                DESCRIPTOR.get_or_init(|| {
                    let mut __fields: Vec<(String, dialog_query::ConceptFieldDescriptor)> =
                        Vec::new();
                    #(#descriptor_pair_pushes)*
                    dialog_query::ConceptDescriptor::try_from(__fields)
                        .expect(
                            "derive(Concept) guarantees at least one required field at compile time",
                        )
                        .with_description(#concept_description_lit)
                })
            }
        }

        // No `From<#struct_name>`/`From<#query_name> for ConceptDescriptor`:
        // a concept type's descriptor is obtained through
        // `Descriptor<ConceptDescriptor>` (or the inherent
        // `#struct_name::descriptor()`), mirroring how attributes
        // expose `Descriptor<AttributeDescriptor>`. The conversions
        // below fill `ConceptQuery.predicate` from that single source.

        // Implement From<Query> for Premise
        impl From<#query_name> for dialog_query::Premise {
            fn from(source: #query_name) -> Self {
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate: <#struct_name as dialog_query::Descriptor<
                        dialog_query::ConceptDescriptor,
                    >>::descriptor()
                    .clone(),
                };
                dialog_query::Premise::Assert(dialog_query::Proposition::Concept(app))
            }
        }

        // Implement From<Query> for Proposition
        impl From<#query_name> for dialog_query::Proposition {
            fn from(source: #query_name) -> Self {
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate: <#struct_name as dialog_query::Descriptor<
                        dialog_query::ConceptDescriptor,
                    >>::descriptor()
                    .clone(),
                };
                dialog_query::Proposition::Concept(app)
            }
        }

        // Implement From<Query> for ConceptQuery
        impl From<#query_name> for dialog_query::ConceptQuery {
            fn from(source: #query_name) -> Self {
                dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate: <#struct_name as dialog_query::Descriptor<
                        dialog_query::ConceptDescriptor,
                    >>::descriptor()
                    .clone(),
                }
            }
        }

        // Implement From<&Query> for ConceptQuery
        impl From<&#query_name> for dialog_query::ConceptQuery {
            fn from(source: &#query_name) -> Self {
                dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate: <#struct_name as dialog_query::Descriptor<
                        dialog_query::ConceptDescriptor,
                    >>::descriptor()
                    .clone(),
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

            fn this(&self) -> dialog_query::Entity {
                <#struct_name as dialog_query::Descriptor<dialog_query::ConceptDescriptor>>::descriptor()
                    .this()
            }
        }

        // Implement IntoIterator to convert concept into attribute statements.
        //
        // Required fields always emit a relation; `Option<T>` fields emit
        // a relation only when `Some(_)`. `None` is *not* persisted;
        // absence is realized as `Option::None` at projection time, never
        // stored as a fact.
        impl IntoIterator for #struct_name {
            type Item = dialog_query::AttributeStatement;
            type IntoIter = std::vec::IntoIter<dialog_query::AttributeStatement>;

            fn into_iter(self) -> Self::IntoIter {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#statement_emits)*
                __statements.into_iter()
            }
        }

        // Implement Statement trait
        impl dialog_query::Statement for #struct_name {
            fn assert(self, update: &mut impl dialog_query::Update) {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#statement_emits)*
                for __s in __statements {
                    dialog_query::Statement::assert(__s, update);
                }
            }

            fn retract(self, update: &mut impl dialog_query::Update) {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#statement_emits)*
                for __s in __statements {
                    dialog_query::Statement::retract(__s, update);
                }
            }
        }

        // Implement Not trait to enable !concept syntax for retraction
        impl std::ops::Not for #struct_name {
            type Output = dialog_query::Retraction<Self>;

            fn not(self) -> Self::Output {
                dialog_query::StatementExt::revert(self)
            }
        }

        impl dialog_query::Predicate for #struct_name {
            type Conclusion = #struct_name;
            type Application = #query_name;
            type Descriptor = dialog_query::ConceptDescriptor;
        }

        // Implement Rule trait: emit one AttributeQuery per
        // field. Required fields pass the user's term through
        // unchanged; optional fields go through
        // `<F as ConceptField>::is_term` which widens the slot's
        // kind to admit the `Nothing` atom. AttributeQuery derives
        // its resolution from that kind, so optional fields end up
        // with set-widened semantics (an Absent fallback row when
        // no fact matches).
        impl #struct_name {
            /// Returns this concept's runtime descriptor (its schema:
            /// attribute set, types, and content hash). Cached; the
            /// canonical way to obtain the descriptor for a concept
            /// type.
            pub fn descriptor() -> &'static dialog_query::ConceptDescriptor {
                <Self as dialog_query::Descriptor<dialog_query::ConceptDescriptor>>::descriptor()
            }

            fn when(terms: dialog_query::Query<Self>) -> dialog_query::Premises {
                let mut selectors: Vec<dialog_query::AttributeQuery> = Vec::new();
                #(
                    {
                        let raw_param = dialog_query::Term::<dialog_query::types::Any>::from(
                            terms.#field_names.clone()
                        );
                        let value_param = <#field_types as dialog_query::ConceptField>::term(raw_param);
                        let descriptor = <<#field_types as dialog_query::ConceptField>::Attribute
                            as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor();
                        let the_term = dialog_query::Term::Constant(
                            dialog_query::Value::from(descriptor.the().clone())
                        );
                        let cardinality = Some(descriptor.cardinality());
                        let query = dialog_query::AttributeQuery::new(
                            the_term,
                            terms.this.clone(),
                            value_param,
                            dialog_query::Term::blank(),
                            cardinality,
                        );
                        selectors.push(query);
                    }
                )*

                selectors.into()
            }
        }
    };

    TokenStream::from(expanded)
}
