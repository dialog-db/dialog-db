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

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, GenericArgument, PathArguments, Type, parse_macro_input};

use super::helpers::extract_doc_comments;

/// If `ty` is syntactically `Option<T>`, return `Some(&T)`. Otherwise
/// `None`. Recognizes the bare `Option<T>`, `std::option::Option<T>`,
/// and `core::option::Option<T>` spellings.
fn extract_option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segments = &type_path.path.segments;
    let last = segments.last()?;
    if last.ident != "Option" {
        return None;
    }
    if segments.len() > 1 {
        let head = &segments[0].ident;
        if head != "std" && head != "core" {
            return None;
        }
    }

    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let arg = args.args.first()?;
    let GenericArgument::Type(inner) = arg else {
        return None;
    };
    Some(inner)
}

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

    // Collect code fragments for each field. Required (non-Option types)
    // and optional (`Option<T>`) fields are tracked in parallel vectors so
    // the descriptor's `with` and `maybe` slots can be populated
    // independently. The query/realize/iterator paths also branch by
    // required vs optional.
    let mut required_query_fields = Vec::new();
    let mut required_field_names = Vec::new();
    let mut required_field_name_lits = Vec::new();
    let mut required_field_types = Vec::new();
    let mut required_realize_fields = Vec::new();
    let mut required_param_inserts = Vec::new();
    let mut required_terms_methods = Vec::new();
    let mut required_descriptor_pairs = Vec::new();
    let mut required_statement_emits = Vec::new();

    let mut optional_query_fields = Vec::new();
    let mut optional_field_names = Vec::new();
    let mut optional_field_name_lits = Vec::new();
    let mut optional_inner_types = Vec::new();
    let mut optional_realize_fields = Vec::new();
    let mut optional_param_inserts = Vec::new();
    let mut optional_terms_methods = Vec::new();
    let mut optional_descriptor_pairs = Vec::new();
    let mut optional_statement_emits = Vec::new();

    let terms_name = syn::Ident::new(&format!("{}Terms", struct_name), struct_name.span());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();

        if field_name_str == "this" {
            continue;
        }

        let field_type = &field.ty;
        let field_name_lit = syn::LitStr::new(&field_name_str, proc_macro2::Span::call_site());

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

        // Detect Option<T> wrapping. The wrapper preserves the optional
        // kind at the Rust type level: the query exposes
        // `Term<Option<Inner::Type>>`, distinguishing it from the
        // non-optional `Term<Inner::Type>`. Presence is carried at
        // runtime by `Binding::Present` / `Binding::Absent`.
        if let Some(inner_type) = extract_option_inner(field_type) {
            let inner_value_ty = quote! { <#inner_type as dialog_query::Attribute>::Type };
            let optional_term_ty = quote! { dialog_query::Term<Option<#inner_value_ty>> };

            optional_query_fields.push(quote! {
                #[doc = #query_field_doc_lit]
                pub #field_name: #optional_term_ty
            });

            optional_terms_methods.push(quote! {
                #[doc = #terms_method_doc_lit]
                pub fn #field_name() -> #optional_term_ty {
                    dialog_query::Term::<Option<#inner_value_ty>>::var(#field_name_lit)
                }
            });

            // Realize: pattern-match on Binding. Present(value) wraps
            // in Some(_); Absent maps to None. Lookup errors (e.g.
            // unbound — no premise produced this variable) propagate.
            //
            // `Term::<Any>::from(&self.#field_name)` uses the
            // `From<&Term<Option<T>>> for Term<Any>` borrow-form
            // conversion to type-erase for lookup.
            optional_realize_fields.push(quote! {
                #field_name: match source.lookup(&dialog_query::Term::<dialog_query::types::Any>::from(&self.#field_name))? {
                    dialog_query::Binding::Present(value) => Some(#inner_type(value.try_into()?)),
                    dialog_query::Binding::Absent => None,
                }
            });

            optional_param_inserts.push(quote! {
                terms.insert(
                    #field_name_lit.into(),
                    dialog_query::Term::<dialog_query::types::Any>::from(source.#field_name),
                );
            });

            optional_descriptor_pairs.push(quote! {
                (
                    #field_name_lit,
                    <#inner_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().clone(),
                )
            });

            // IntoIterator / Statement: emit a relation only when `Some(_)`.
            // None means "don't persist None" — there is no fact to record.
            optional_statement_emits.push(quote! {
                if let Some(__inner) = self.#field_name.as_ref() {
                    let __expr = dialog_query::attribute::expression::dynamic::DynamicAttributeExpression {
                        the: <#inner_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().the().clone(),
                        of: self.this.clone(),
                        is: dialog_query::Value::from(<#inner_type as dialog_query::Attribute>::value(__inner).clone()),
                        cause: None,
                        cardinality: Some(<#inner_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().cardinality()),
                    };
                    __statements.push(__expr.into());
                }
            });

            optional_field_names.push(field_name);
            optional_field_name_lits.push(field_name_lit);
            optional_inner_types.push(inner_type);
            continue;
        }

        // Required (non-Option) field path.
        let inner_value_ty = quote! { <#field_type as dialog_query::Attribute>::Type };

        required_query_fields.push(quote! {
            #[doc = #query_field_doc_lit]
            pub #field_name: dialog_query::Term<#inner_value_ty>
        });

        required_terms_methods.push(quote! {
            #[doc = #terms_method_doc_lit]
            pub fn #field_name() -> dialog_query::Term<#inner_value_ty> {
                dialog_query::Term::<#inner_value_ty>::var(#field_name_lit)
            }
        });

        required_realize_fields.push(quote! {
            #field_name: #field_type(
                source.lookup(&dialog_query::Term::from(&self.#field_name))?.content()?.try_into()?
            )
        });

        required_param_inserts.push(quote! {
            terms.insert(
                #field_name_lit.into(),
                dialog_query::Term::<dialog_query::types::Any>::from(source.#field_name),
            );
        });

        required_descriptor_pairs.push(quote! {
            (
                #field_name_lit,
                <#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().clone(),
            )
        });

        required_statement_emits.push(quote! {
            {
                let __expr = dialog_query::attribute::expression::dynamic::DynamicAttributeExpression {
                    the: <#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().the().clone(),
                    of: self.this.clone(),
                    is: dialog_query::Value::from(<#field_type as dialog_query::Attribute>::value(&self.#field_name).clone()),
                    cause: None,
                    cardinality: Some(<#field_type as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().cardinality()),
                };
                __statements.push(__expr.into());
            }
        });

        required_field_names.push(field_name);
        required_field_name_lits.push(field_name_lit);
        required_field_types.push(field_type);
    }

    // The `assert_implements_attribute` validation needs to run on every
    // attribute type, including those wrapped in `Option<T>`. Build a
    // unified list of types to validate.
    let validated_types: Vec<_> = required_field_types
        .iter()
        .map(|t| quote! { #t })
        .chain(optional_inner_types.iter().map(|t| quote! { #t }))
        .collect();

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
        // Compile-time validation that all fields (except 'this') implement
        // Attribute + Descriptor. Includes the inner type of any `Option<T>`
        // field — `Option<T>` itself is not an Attribute.
        const _: () = {
            fn assert_implements_attribute<T: dialog_query::Attribute + dialog_query::Descriptor<dialog_query::AttributeDescriptor>>() {}
            fn #validate_fn_name() {
                #(assert_implements_attribute::<#validated_types>();)*
            }
        };

        #[doc = #query_struct_doc]
        #[derive(Debug, Clone, PartialEq)]
        pub struct #query_name {
            #[doc = #query_this_field_doc]
            pub this: dialog_query::Term<dialog_query::Entity>,
            #(#required_query_fields,)*
            #(#optional_query_fields,)*
        }

        impl Default for #query_name {
            fn default() -> Self {
                Self {
                    this: dialog_query::Term::var("this"),
                    #(#required_field_names: dialog_query::Term::var(#required_field_name_lits),)*
                    #(#optional_field_names: dialog_query::Term::var(#optional_field_name_lits),)*
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
            #(#required_terms_methods)*
            #(#optional_terms_methods)*
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
                    #(#required_realize_fields,)*
                    #(#optional_realize_fields,)*
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

                #(#required_param_inserts)*
                #(#optional_param_inserts)*

                terms
            }
        }

        // Implement From<StructName> for ConceptDescriptor.
        //
        // Required fields populate `with`; `Option<T>` fields populate
        // `maybe`. The two slots are independent, and a concept may have
        // either or both. The `Vec<(&str, AttributeDescriptor)>` annotation
        // is necessary so type inference works when the optional list is
        // empty (no `Option<T>` fields on the struct).
        impl From<#struct_name> for dialog_query::ConceptDescriptor {
            fn from(_: #struct_name) -> Self {
                let __maybe: Vec<(&str, dialog_query::AttributeDescriptor)> = vec![
                    #(#optional_descriptor_pairs),*
                ];
                dialog_query::ConceptDescriptor::from(vec![
                    #(#required_descriptor_pairs),*
                ])
                .with_maybe(__maybe)
            }
        }

        // Implement From<Query> for ConceptDescriptor
        impl From<#query_name> for dialog_query::ConceptDescriptor {
            fn from(_: #query_name) -> Self {
                let __maybe: Vec<(&str, dialog_query::AttributeDescriptor)> = vec![
                    #(#optional_descriptor_pairs),*
                ];
                dialog_query::ConceptDescriptor::from(vec![
                    #(#required_descriptor_pairs),*
                ])
                .with_maybe(__maybe)
            }
        }

        // Implement From<Query> for Premise
        impl From<#query_name> for dialog_query::Premise {
            fn from(source: #query_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                };
                dialog_query::Premise::Assert(dialog_query::Proposition::Concept(app))
            }
        }

        // Implement From<Query> for Proposition
        impl From<#query_name> for dialog_query::Proposition {
            fn from(source: #query_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                let app = dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                };
                dialog_query::Proposition::Concept(app)
            }
        }

        // Implement From<Query> for ConceptQuery
        impl From<#query_name> for dialog_query::ConceptQuery {
            fn from(source: #query_name) -> Self {
                let predicate: dialog_query::ConceptDescriptor = source.clone().into();
                dialog_query::ConceptQuery {
                    terms: source.into(),
                    predicate,
                }
            }
        }

        // Implement From<&Query> for ConceptQuery
        impl From<&#query_name> for dialog_query::ConceptQuery {
            fn from(source: &#query_name) -> Self {
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

            fn this(&self) -> dialog_query::Entity {
                let predicate: dialog_query::ConceptDescriptor = self.clone().into();
                predicate.this()
            }
        }

        // Implement IntoIterator to convert concept into attribute statements.
        //
        // Required fields always emit a relation; `Option<T>` fields emit
        // a relation only when `Some(_)`. `None` is *not* persisted —
        // absence is realized as `Option::None` at projection time, never
        // stored as a fact.
        impl IntoIterator for #struct_name {
            type Item = dialog_query::AttributeStatement;
            type IntoIter = std::vec::IntoIter<dialog_query::AttributeStatement>;

            fn into_iter(self) -> Self::IntoIter {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#required_statement_emits)*
                #(#optional_statement_emits)*
                __statements.into_iter()
            }
        }

        // Implement Statement trait
        impl dialog_query::Statement for #struct_name {
            fn assert(self, update: &mut impl dialog_query::Update) {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#required_statement_emits)*
                #(#optional_statement_emits)*
                for __s in __statements {
                    dialog_query::Statement::assert(__s, update);
                }
            }

            fn retract(self, update: &mut impl dialog_query::Update) {
                let mut __statements: Vec<dialog_query::AttributeStatement> = Vec::new();
                #(#required_statement_emits)*
                #(#optional_statement_emits)*
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

        // Implement Rule trait — the body lists only required attribute
        // queries. Optional (`Option<T>`) fields are resolved by a
        // separate optional-resolution premise inside the concept
        // descriptor's deductive-rule projection (see
        // `From<&ConceptDescriptor> for DeductiveRule`).
        impl #struct_name {
            fn when(terms: dialog_query::Query<Self>) -> dialog_query::Premises {
                let selectors: Vec<dialog_query::AttributeQuery> = vec![
                    #(
                        {
                            let value_param = dialog_query::Term::<dialog_query::types::Any>::from(
                                terms.#required_field_names.clone()
                            );

                            dialog_query::AttributeQuery::new(
                                dialog_query::Term::Constant(dialog_query::Value::from(
                                    <#required_field_types as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().the().clone()
                                )),
                                terms.this.clone(),
                                value_param,
                                dialog_query::Term::blank(),
                                Some(<#required_field_types as dialog_query::Descriptor<dialog_query::AttributeDescriptor>>::descriptor().cardinality()),
                            )
                        }
                    ),*
                ];

                selectors.into()
            }
        }
    };

    TokenStream::from(expanded)
}
