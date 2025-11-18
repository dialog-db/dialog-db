//! Procedural macros for generating concept definitions

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Expr, Fields, Lit, Meta, Type};

/// Derive macro to generate Concept implementation from a struct definition.
///
/// This macro generates all the necessary boilerplate for implementing a concept,
/// including Match, Assert, Retract, and Attributes types.
///
/// # Example
///
/// This macro transforms input like:
/// ```text
/// use dialog_query::concept::Concept as ConceptTrait;
/// use dialog_query::rule::Rule as RuleTrait;
/// use dialog_query::Term;
/// use dialog_query_macros::Concept;
///
/// #[derive(Concept, Debug, Clone)]
/// pub struct Person {
///     /// Name of the person
///     pub name: String,
///     /// Birthday of the person
///     pub birthday: u32,
/// }
///
/// // The generated code can be used like this:
/// let entity = Term::var("person_entity");
/// let person_query = Person::r#match(entity);
/// assert_eq!(Person::name(), "person");
/// ```
///
/// Into generated code that creates:
/// - PersonMatch struct for querying
/// - PersonAssert struct for conclusions
/// - PersonRetract struct for retractions
/// - PersonAttributes struct for fluent queries
/// - Static attribute constants like PERSON_NAME, PERSON_BIRTHDAY
/// - Concept and Rule trait implementations
///
/// To see complete working examples with the generated code, check the tests in the main dialog-query crate.
///
/// # Generated Types
///
/// For a struct `Person` with fields `name: String` and `birthday: u32`, this generates:
/// - `PersonMatch`: Query pattern with `Term<String>` and `Term<u32>` fields
/// - `PersonAssert`: Assertion pattern for rule conclusions
/// - `PersonRetract`: Retraction pattern for removing facts
/// - `PersonAttributes`: Fluent query builder with type-safe attribute matchers
/// - `PERSON_NAME`: Static attribute constant for the name field
/// - `PERSON_BIRTHDAY`: Static attribute constant for the birthday field
#[proc_macro_derive(Concept)]
pub fn derive_concept(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Extract doc comments from the concept struct
    let concept_description = extract_doc_comments(&input.attrs);
    let concept_description_lit =
        syn::LitStr::new(&concept_description, proc_macro2::Span::call_site());

    // Extract fields from the ostruct
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

    // Check for required `this: Entity` field
    let has_this_field = fields.iter().any(|field| {
        if let Some(field_name) = &field.ident {
            if field_name == "this" {
                // Check if the type is Entity
                if let Type::Path(type_path) = &field.ty {
                    if let Some(last_segment) = type_path.path.segments.last() {
                        return last_segment.ident == "Entity";
                    }
                }
            }
        }
        false
    });

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

    // Extract field information
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
        // Actual attribute description comes from the Attribute trait
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
        // Use a normalized name for the static (since we can't use the field type's path)
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
        // The field type implements Attribute, so we extract all metadata from its methods
        // Uses LazyLock since methods may compute values at runtime
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

        // Generate Attribute<Value> for the attributes() method - extracts from Attribute trait methods
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

        // Generate rule when field conversion - use Attribute's selector()
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
                // For now, return None - proper implementation would need term conversion storage
                None
            }
        });

        // Generate attribute tuples for Attributes implementation - use Attribute metadata methods
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

        // Generate Relation for IntoIterator implementation - extract value from Attribute
        instance_relations.push(quote! {
            dialog_query::Relation::new(
                <#field_type as dialog_query::Attribute>::selector(),
                self.this.clone(),
                dialog_query::types::Scalar::as_value(<#field_type as dialog_query::Attribute>::value(&self.#field_name)),
            )
        });
    }

    // Generate type names based on struct name (e.g., Person -> PersonMatch, PersonTerms, etc.)
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
                // Each field type must implement Attribute trait
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
                    // Wrap each extracted value in its Attribute constructor
                    #(#field_names: #field_types(source.get(&self.#field_names)?)),*
                })
            }
        }

        // Add inherent query method so users don't need to import Match trait
        impl #match_name {
            fn realize(&self, source: dialog_query::selection::Answer) -> std::result::Result<#struct_name, dialog_query::QueryError> {
                dialog_query::concept::Match::realize(self, source)
            }

            /// Query for instances matching this pattern
            ///
            /// This is a convenience method that delegates to the Match trait's query method.
            /// It allows calling query without importing the Match trait.
            pub fn query<S: dialog_query::query::Source>(
                &self,
                source: S,
            ) -> impl dialog_query::query::Output<#struct_name> {
                use futures_util::StreamExt;
                // .into() works cleanly - the generated From<#match_name> for ConceptApplication handles it
                let application: dialog_query::application::concept::ConceptApplication = self.clone().into();
                let cloned = self.clone();
                application
                    .query(source)
                    .map(move |input| cloned.realize(input?))
            }
        }

        // Implement From<Match> for Parameters to satisfy Into<Parameters> bound
        impl From<#match_name> for dialog_query::Parameters {
            fn from(source: #match_name) -> Self {
                let mut terms = Self::new();

                terms.insert("this".into(), source.this.as_unknown());

                // Insert each attribute field with term conversion
                #(terms.insert(#field_name_lits.into(), source.#field_names.as_unknown());)*

                terms
            }
        }

        // Implement From<Match> for Premise - enables Match::<Concept> { ... } in When::from([...])
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

        // Implement From<Match> for ConceptApplication - enables .into() without type annotation
        impl From<#match_name> for dialog_query::application::concept::ConceptApplication {
            fn from(source: #match_name) -> Self {
                dialog_query::application::concept::ConceptApplication {
                    terms: source.into(),
                    concept: #struct_name::CONCEPT,
                }
            }
        }

        // Implement From<&Match> for ConceptApplication - enables .into() on references
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
            fn this(&self) -> dialog_query::artifact::Entity {
                self.this.clone()
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

        // Implement Claim trait - direct implementation without iterator overhead
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
                // Create fact selectors for each attribute with type conversion
                let selectors = vec![
                    #(#rule_when_fields),*
                ];

                // Return When collection with all selectors
                selectors.into()
            }
        }
    };

    TokenStream::from(expanded)
}

/// Generate the Type value for a field type in static attribute declarations.
///
/// This uses the IntoType trait's const TYPE associated constant to determine
/// the type at compile time. This works because IntoType::TYPE is a const,
/// allowing proper type detection without string matching.
///
/// Returns Option<Type> directly from the trait's TYPE constant:
/// - Some(Type::String) for String types
/// - None for Value types (accepts any type)
fn type_to_value_data_type(ty: &Type) -> proc_macro2::TokenStream {
    quote! {
        <#ty as dialog_query::types::IntoType>::TYPE
    }
}

/// Extract doc comments from attributes
fn extract_doc_comments(attrs: &[Attribute]) -> String {
    let mut docs = Vec::new();

    for attr in attrs {
        match &attr.meta {
            Meta::NameValue(nv) if nv.path.is_ident("doc") => {
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit) = &expr_lit.lit {
                        // Trim leading space that rustdoc adds
                        let doc = lit.value();
                        let trimmed = doc.trim_start_matches(' ');
                        docs.push(trimmed.to_string());
                    }
                }
            }
            _ => {}
        }
    }

    // Join multiple doc comment lines with spaces and trim
    docs.join(" ").trim().to_string()
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for ch in s.chars() {
        if ch.is_uppercase() {
            if !result.is_empty() {
                result.push('.');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }

    result
}

/// Convert PascalCase or snake_case to kebab-case at compile time
/// Examples:
/// - UserName -> user-name
/// - HTTPRequest -> http-request
/// - account_name -> account-name
fn to_kebab_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_is_lower = false;
    let mut prev_is_upper = false;

    for (i, ch) in s.chars().enumerate() {
        if ch == '_' {
            result.push('-');
            prev_is_lower = false;
            prev_is_upper = false;
        } else if ch.is_uppercase() {
            // Add hyphen before uppercase if:
            // 1. Not at start
            // 2. Previous was lowercase (camelCase boundary)
            // 3. Next is lowercase and previous was uppercase (HTTPRequest -> http-request)
            if i > 0
                && (prev_is_lower
                    || (prev_is_upper && s.chars().nth(i + 1).is_some_and(|c| c.is_lowercase())))
            {
                result.push('-');
            }
            result.push(ch.to_lowercase().next().unwrap());
            prev_is_lower = false;
            prev_is_upper = true;
        } else {
            result.push(ch);
            prev_is_lower = true;
            prev_is_upper = false;
        }
    }

    result
}

/// Derive macro to generate Formula implementation from a struct definition.
///
/// This macro generates all the necessary boilerplate for implementing a formula,
/// automatically determining which fields are inputs vs. derived outputs.
///
/// # Example
///
/// ```ignore
/// use dialog_query::{Formula, Input};
///
/// #[derive(Debug, Clone, Formula)]
/// pub struct Sum {
///     pub of: u32,
///     pub with: u32,
///     #[derived(cost = 5)]
///     pub is: u32,
/// }
///
/// impl Sum {
///     fn derive(input: Input<Self>) -> Vec<Self> {
///         vec![Sum {
///             of: input.of,
///             with: input.with,
///             is: input.of + input.with,
///         }]
///     }
/// }
/// ```
///
/// # Attributes
///
/// - `#[derived]` or `#[derived(cost = N)]` - Mark fields as derived/computed (not inputs)
///   - If cost is omitted, defaults to 1
///   - Total formula cost is the sum of all derived field costs
///
/// # Generated Code
///
/// For a struct `Sum`, this generates:
/// - `SumInput` - Struct with only non-derived fields (inputs)
/// - `SumMatch` - Pattern struct with all fields as `Term<T>`
/// - `impl Formula for Sum` - All required trait methods
/// - `impl Output for Sum` - Auto-generated write() for derived fields
/// - `impl Pattern for Sum` - Pattern support
/// - `impl formula::Match for SumMatch` - Match pattern support
/// - `impl From<SumMatch> for Parameters` - Parameter conversion
/// - `impl TryFrom<&mut Cursor> for SumInput` - Input parsing
#[proc_macro_derive(Formula, attributes(derived))]
pub fn derive_formula(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Extract fields from the struct
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "Formula can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "Formula can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    // Parse fields and identify which are derived (with optional cost)
    let mut input_fields = Vec::new();
    let mut derived_fields = Vec::new(); // (name, type, doc, cost)
    let mut all_fields = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        // Extract doc comment
        let doc_comment = extract_doc_comments(&field.attrs);

        // Check if field has #[derived] and parse optional cost
        let derived_info = parse_derived_attribute(&field.attrs);

        if let Some(cost) = derived_info {
            all_fields.push((field_name, field_type, doc_comment.clone(), true, cost));
            derived_fields.push((field_name, field_type, doc_comment, cost));
        } else {
            all_fields.push((field_name, field_type, doc_comment.clone(), false, 0));
            input_fields.push((field_name, field_type, doc_comment));
        }
    }

    // Validate at least one derived field exists
    if derived_fields.is_empty() {
        return syn::Error::new_spanned(
            &input,
            "Formula must have at least one field marked with #[derived]",
        )
        .to_compile_error()
        .into();
    }

    // Calculate total formula cost by summing derived field costs
    let total_cost: usize = derived_fields.iter().map(|(_, _, _, cost)| cost).sum();

    // Generate type names
    let input_name = syn::Ident::new(&format!("{}Input", struct_name), struct_name.span());
    let match_name = syn::Ident::new(&format!("{}Match", struct_name), struct_name.span());
    let cells_name = syn::Ident::new(
        &format!("{}_CELLS", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate operator name (snake_case)
    let operator_name = to_snake_case(&struct_name.to_string());
    let operator_lit = syn::LitStr::new(&operator_name, proc_macro2::Span::call_site());

    // Generate Input struct fields (only non-derived fields)
    let input_struct_fields: Vec<_> = input_fields
        .iter()
        .map(|(name, ty, doc)| {
            let doc_lit = syn::LitStr::new(doc, proc_macro2::Span::call_site());
            quote! {
                #[doc = #doc_lit]
                pub #name: #ty
            }
        })
        .collect();

    // Generate Match struct fields (all fields as Term<T>)
    let match_struct_fields: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, doc, _is_derived, _cost)| {
            let doc_lit = syn::LitStr::new(doc, proc_macro2::Span::call_site());
            quote! {
                #[doc = #doc_lit]
                pub #name: dialog_query::term::Term<#ty>
            }
        })
        .collect();

    // Generate cells definition
    let cell_definitions: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, doc, is_derived, cost)| {
            let name_str = name.to_string();
            let name_lit = syn::LitStr::new(&name_str, proc_macro2::Span::call_site());
            let doc_lit = syn::LitStr::new(doc, proc_macro2::Span::call_site());
            let data_type = type_to_value_data_type(ty);

            if *is_derived {
                quote! {
                    builder
                        .cell(#name_lit, #data_type)
                        .the(#doc_lit)
                        .derived(#cost);
                }
            } else {
                quote! {
                    builder
                        .cell(#name_lit, #data_type)
                        .the(#doc_lit)
                        .required();
                }
            }
        })
        .collect();

    // Generate field names for Input TryFrom
    let input_field_names: Vec<_> = input_fields.iter().map(|(name, _, _)| name).collect();
    let input_field_name_lits: Vec<_> = input_fields
        .iter()
        .map(|(name, _, _)| {
            let name_str = name.to_string();
            syn::LitStr::new(&name_str, proc_macro2::Span::call_site())
        })
        .collect();

    // Generate field names for Match Into<Parameters>
    let all_field_names: Vec<_> = all_fields.iter().map(|(name, _, _, _, _)| name).collect();
    let all_field_name_lits: Vec<_> = all_fields
        .iter()
        .map(|(name, _, _, _, _)| {
            let name_str = name.to_string();
            syn::LitStr::new(&name_str, proc_macro2::Span::call_site())
        })
        .collect();

    // Generate Output::write() - writes all derived fields
    let write_statements: Vec<_> = derived_fields
        .iter()
        .map(|(name, _ty, _, _cost)| {
            let name_str = name.to_string();
            let name_lit = syn::LitStr::new(&name_str, proc_macro2::Span::call_site());
            quote! {
                cursor.write(#name_lit, &self.#name.clone().into())?;
            }
        })
        .collect();

    let expanded = quote! {
        /// Input structure for #struct_name formula
        ///
        /// Contains only the required (non-derived) fields that must be provided
        /// to compute the formula.
        #[derive(Debug, Clone)]
        pub struct #input_name {
            #(#input_struct_fields),*
        }

        /// Match pattern for #struct_name formula
        ///
        /// Contains all fields (both input and derived) as Term<T> for pattern matching
        /// in queries.
        #[derive(Debug, Clone)]
        pub struct #match_name {
            #(#match_struct_fields),*
        }

        /// Static storage for formula cells
        static #cells_name: ::std::sync::OnceLock<dialog_query::predicate::formula::Cells> = ::std::sync::OnceLock::new();

        impl dialog_query::dsl::Quarriable for #struct_name {
            type Query = #match_name;
        }

        impl dialog_query::predicate::formula::Match for #match_name {
            type Formula = #struct_name;
        }

        impl ::std::convert::From<#match_name> for dialog_query::Parameters {
            fn from(terms: #match_name) -> Self {
                let mut parameters = Self::new();
                #(parameters.insert(#all_field_name_lits.into(), terms.#all_field_names.as_unknown());)*
                parameters
            }
        }

        impl ::std::convert::TryFrom<&mut dialog_query::cursor::Cursor> for #input_name {
            type Error = dialog_query::error::FormulaEvaluationError;

            fn try_from(cursor: &mut dialog_query::cursor::Cursor) -> ::std::result::Result<Self, Self::Error> {
                Ok(#input_name {
                    #(#input_field_names: cursor.resolve(#input_field_name_lits)?.try_into()?),*
                })
            }
        }

        impl dialog_query::predicate::formula::Output for #struct_name {
            fn write(&self, cursor: &mut dialog_query::cursor::Cursor) -> ::std::result::Result<(), dialog_query::error::FormulaEvaluationError> {
                #(#write_statements)*
                ::std::result::Result::Ok(())
            }
        }

        impl dialog_query::predicate::formula::Formula for #struct_name {
            type Input = #input_name;
            type Match = #match_name;

            fn operator() -> &'static str {
                #operator_lit
            }

            fn cells() -> &'static dialog_query::predicate::formula::Cells {
                #cells_name.get_or_init(|| {
                    dialog_query::predicate::formula::Cells::define(|builder| {
                        #(#cell_definitions)*
                    })
                })
            }

            fn cost() -> usize {
                #total_cost
            }

            // fn dependencies() -> dialog_query::Dependencies {
            //     let mut dependencies = dialog_query::Dependencies::new();
            //     #(#required_fields)*
            //     #(#provided_fields)*
            //     dependencies
            // }

            fn derive(input: Self::Input) -> ::std::vec::Vec<Self> {
                #struct_name::derive(input)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Parse the #[derived] or #[derived(cost = N)] attribute
/// Returns Some(cost) if the field is derived, None otherwise
/// Default cost is 1 if not specified
fn parse_derived_attribute(attrs: &[Attribute]) -> Option<usize> {
    for attr in attrs {
        if attr.path().is_ident("derived") {
            // Check if there are any nested meta items
            let mut cost = Some(1); // Default cost is 1

            // Try to parse nested meta (cost = N)
            let result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("cost") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Int(lit_int) = lit {
                        cost = Some(lit_int.base10_parse::<usize>()?);
                        Ok(())
                    } else {
                        Err(meta.error("cost must be an integer"))
                    }
                } else {
                    Err(meta.error("unknown derived attribute parameter"))
                }
            });

            // If parsing succeeds or there's no nested content, return the cost
            // If parsing fails, it's an error in the attribute syntax
            match result {
                Ok(()) => return cost,
                Err(_) if matches!(attr.meta, syn::Meta::Path(_)) => {
                    // Just #[derived] with no parameters - use default cost
                    return Some(1);
                }
                Err(e) => {
                    // Syntax error in attribute
                    panic!("Error parsing derived attribute: {}", e);
                }
            }
        }
    }
    None
}

/// Derive macro for the Attribute trait
///
/// Generates an implementation of the `dialog_query::attribute::Attribute` trait
/// for tuple structs that wrap a single Scalar value.
///
/// # Example
///
/// ```ignore
/// mod employee {
///     use dialog_query::attribute::Attribute;
///
///     /// Name of the employee
///     #[derive(Attribute)]
///     pub struct Name(String);
///
///     /// Employees managed by this entity
///     #[derive(Attribute)]
///     #[cardinality(many)]
///     pub struct Manages(Entity);
/// }
/// ```
///
/// # Attributes
///
/// - `#[cardinality(many)]` - Marks the attribute as having many values (defaults to One)
/// - `#[namespace = "custom"]` - Override the default namespace (defaults to lowercase struct name)
///
/// # Generated Implementation
///
/// The macro generates:
/// - `namespace()` - Returns the namespace (defaults to lowercase struct name)
/// - `name()` - Returns the attribute name (lowercase struct name)
/// - `description()` - Returns doc comment text
/// - `cardinality()` - Returns cardinality (One or Many)
/// - `value()` - Returns reference to the wrapped value
/// - `selector()` - Returns the full attribute selector (namespace/name)
#[proc_macro_derive(Attribute, attributes(cardinality, namespace))]
pub fn derive_attribute(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Parse tuple struct with single field
    let wrapped_type = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            Fields::Unnamed(_) => {
                return syn::Error::new_spanned(
                    &input,
                    "Attribute can only be derived for tuple structs with exactly one field",
                )
                .to_compile_error()
                .into();
            }
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "Attribute can only be derived for tuple structs (e.g., struct Name(String))",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input,
                "Attribute can only be derived for tuple structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // Check if namespace is explicitly specified
    let explicit_namespace = parse_namespace_attribute(&input.attrs);

    // Extract attribute name (convert PascalCase/snake_case to kebab-case)
    let attr_name = to_kebab_case(&struct_name.to_string());
    let attr_name_lit = syn::LitStr::new(&attr_name, proc_macro2::Span::call_site());

    // Extract doc comments
    let description = extract_doc_comments(&input.attrs);
    let description_lit = syn::LitStr::new(&description, proc_macro2::Span::call_site());

    // Parse cardinality
    let cardinality = parse_cardinality_attribute(&input.attrs);

    // Create static schema name (unused currently, but may be needed in the future)
    let _schema_name = syn::Ident::new(
        &format!("{}_SCHEMA", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate namespace static names (unique per struct)
    let compute_len_name = syn::Ident::new(
        &format!(
            "__compute_{}_namespace_len",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let compute_bytes_name = syn::Ident::new(
        &format!(
            "__compute_{}_namespace_bytes",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let namespace_len_name = syn::Ident::new(
        &format!("__{}_NAMESPACE_LEN", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let namespace_bytes_name = syn::Ident::new(
        &format!("{}_NAMESPACE_BYTES", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let namespace_name = syn::Ident::new(
        &format!("{}_NAMESPACE", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate additional const name for module path
    let module_path_const_name = syn::Ident::new(
        &format!("__{}_MODULE_PATH", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate namespace - explicit or derived
    let (namespace_static_decl, namespace_expr) = if let Some(ref ns) = explicit_namespace {
        let ns_lit = syn::LitStr::new(ns, proc_macro2::Span::call_site());
        (quote! {}, quote! { #ns_lit })
    } else {
        // For derived namespaces: use const fn with const-compatible str construction
        (
            quote! {
                // Capture module_path!() in a const to avoid temporary value issues
                const #module_path_const_name: &str = module_path!();

                const fn #compute_len_name(path: &str) -> usize {
                    let bytes = path.as_bytes();

                    // Find the last segment (after the last ::)
                    let mut last_sep_pos = 0;
                    let mut i = 0;
                    while i < bytes.len() {
                        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                            last_sep_pos = i + 2;
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }

                    // Count length from last separator to end
                    bytes.len() - last_sep_pos
                }

                const fn #compute_bytes_name<const N: usize>(path: &str) -> [u8; N] {
                    let mut result = [0u8; N];
                    let bytes = path.as_bytes();

                    // Find the last segment (after the last ::)
                    let mut last_sep_pos = 0;
                    let mut i = 0;
                    while i < bytes.len() {
                        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                            last_sep_pos = i + 2;
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }

                    // Copy last segment, converting underscore to hyphen
                    let mut out = 0;
                    i = last_sep_pos;
                    while i < bytes.len() && out < N {
                        let byte = if bytes[i] == b'_' { b'-' } else { bytes[i] };
                        result[out] = byte;
                        out += 1;
                        i += 1;
                    }

                    result
                }

                #[allow(non_snake_case)]
                const fn #namespace_name<const N: usize>(bytes: &[u8; N]) -> &str {
                    // SAFETY: We only insert valid UTF-8 bytes (ASCII letters, hyphens)
                    // in compute_bytes_name, so this is guaranteed to be valid UTF-8
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                }

                const #namespace_len_name: usize = #compute_len_name(#module_path_const_name);
                const #namespace_bytes_name: [u8; #namespace_len_name] = #compute_bytes_name(#module_path_const_name);
            },
            quote! { #namespace_name(&#namespace_bytes_name) },
        )
    };

    // Generate concept const name
    let concept_const_name = syn::Ident::new(
        &format!("{}_CONCEPT", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    let expanded = quote! {
        #namespace_static_decl

        // Generate the CONCEPT constant
        const #concept_const_name: dialog_query::predicate::concept::Concept = {
            const ATTRS: dialog_query::predicate::concept::Attributes =
                dialog_query::predicate::concept::Attributes::Static(&[(
                    "has",  // Use "has" as the parameter key to match With<A> field name
                    dialog_query::attribute::AttributeSchema {
                        namespace: #namespace_expr,
                        name: #attr_name_lit,
                        description: #description_lit,
                        cardinality: #cardinality,
                        content_type: <#wrapped_type as dialog_query::types::IntoType>::TYPE,
                        marker: std::marker::PhantomData,
                    },
                )]);

            dialog_query::predicate::concept::Concept::Static {
                description: #description_lit,
                attributes: &ATTRS,
            }
        };

        impl dialog_query::attribute::Attribute for #struct_name {
            type Type = #wrapped_type;

            // Associated types pointing to generic With types
            type Match = dialog_query::attribute::WithMatch<Self>;
            type Instance = dialog_query::attribute::With<Self>;
            type Term = dialog_query::attribute::WithTerms<Self>;

            const NAMESPACE: &'static str = #namespace_expr;
            const NAME: &'static str = #attr_name_lit;
            const DESCRIPTION: &'static str = #description_lit;
            const CARDINALITY: dialog_query::attribute::Cardinality = #cardinality;
            const SCHEMA: dialog_query::attribute::AttributeSchema<Self::Type> = dialog_query::attribute::AttributeSchema {
                namespace: Self::NAMESPACE,
                name: Self::NAME,
                description: Self::DESCRIPTION,
                cardinality: Self::CARDINALITY,
                content_type: <#wrapped_type as dialog_query::types::IntoType>::TYPE,
                marker: std::marker::PhantomData,
            };
            const CONCEPT: dialog_query::predicate::concept::Concept = #concept_const_name;

            fn value(&self) -> &Self::Type {
                &self.0
            }

            fn new(value: Self::Type) -> Self {
                Self(value)
            }
        }

        // Debug implementation showing attribute metadata
        impl std::fmt::Debug for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!(#struct_name))
                    .field("namespace", &<Self as dialog_query::attribute::Attribute>::NAMESPACE)
                    .field("name", &<Self as dialog_query::attribute::Attribute>::NAME)
                    .field("value", &self.0)
                    .finish()
            }
        }

        // Display implementation showing selector and value
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}/{}: {:?}",
                    <Self as dialog_query::attribute::Attribute>::NAMESPACE,
                    <Self as dialog_query::attribute::Attribute>::NAME,
                    self.0
                )
            }
        }

        // Generic From implementation for any type that can convert into the wrapped type
        impl<U: ::std::convert::Into<#wrapped_type>> ::std::convert::From<U> for #struct_name {
            fn from(value: U) -> Self {
                <Self as dialog_query::attribute::Attribute>::new(value.into())
            }
        }
    };

    TokenStream::from(expanded)
}

/// Parse the #[namespace = "..."] attribute
/// Returns Some(namespace) if specified, None to use default
fn parse_namespace_attribute(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("namespace") {
            if let Meta::NameValue(nv) = &attr.meta {
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit) = &expr_lit.lit {
                        return Some(lit.value());
                    }
                }
            }
        }
    }
    None
}

/// Parse the #[cardinality(many)] attribute
/// Returns the appropriate Cardinality reference
fn parse_cardinality_attribute(attrs: &[Attribute]) -> proc_macro2::TokenStream {
    for attr in attrs {
        if attr.path().is_ident("cardinality") {
            let result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("many") || meta.path.is_ident("one") {
                    Ok(())
                } else {
                    Err(meta.error("cardinality must be 'one' or 'many'"))
                }
            });

            match result {
                Ok(()) => {
                    // Check which one it was
                    if let Meta::List(list) = &attr.meta {
                        let tokens_str = list.tokens.to_string();
                        if tokens_str.contains("many") {
                            return quote! { dialog_query::attribute::Cardinality::Many };
                        }
                    }
                }
                Err(e) => {
                    panic!("Error parsing cardinality attribute: {}", e);
                }
            }
        }
    }

    // Default to One
    quote! { dialog_query::attribute::Cardinality::One }
}
