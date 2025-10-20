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
    let mut assert_fields = Vec::new();
    let mut retract_fields = Vec::new();
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

        // Extract doc comment for the field
        let doc_comment = extract_doc_comments(&field.attrs);
        let doc_comment_lit = syn::LitStr::new(&doc_comment, proc_macro2::Span::call_site());

        // Generate Match field (Term<T>)
        match_fields.push(quote! {
            #[doc = #doc_comment_lit]
            pub #field_name: dialog_query::term::Term<#field_type>
        });

        terms_methods.push(quote! {
            impl #terms_name {
                pub fn #field_name() -> dialog_query::Term<#field_type> {
                    dialog_query::Term::<#field_type>::var(#field_name_lit)
                }
            }
        });

        // Generate Attributes field (Match<T>)
        attributes_fields.push(quote! {
            #[doc = #doc_comment_lit]
            pub #field_name: dialog_query::attribute::Match<#field_type>
        });

        // Generate Assert field (Term<T>)
        assert_fields.push(quote! {
            pub #field_name: dialog_query::term::Term<#field_type>
        });

        // Generate Retract field (Term<T>)
        retract_fields.push(quote! {
            pub #field_name: dialog_query::term::Term<#field_type>
        });

        // Generate attribute initialization for Attributes
        let namespace_for_prefix = namespace.replace(".", "_");
        let prefixed_field_name = syn::Ident::new(
            &format!(
                "{}_{}",
                namespace_for_prefix.to_uppercase(),
                field_name_str.to_uppercase()
            ),
            field_name.span(),
        );
        attribute_init_fields.push(quote! {
            #field_name: #prefixed_field_name.of(entity.clone())
        });

        // Get the compile-time data type for this field
        let data_type_value = type_to_value_data_type(field_type);
        typed_attributes.push(quote! {
            /// Static attribute definition for #field_name
            pub static #prefixed_field_name: dialog_query::attribute::Attribute<#field_type> = dialog_query::attribute::Attribute {
                namespace: #namespace_lit,
                name: #field_name_lit,
                description: #doc_comment_lit,
                cardinality: dialog_query::attribute::Cardinality::One,
                content_type: #data_type_value,
                marker: std::marker::PhantomData,
            };
        });

        // Generate Attribute<Value> for the attributes() method
        value_attributes.push(quote! {
            dialog_query::attribute::Attribute {
                namespace: #namespace_lit,
                name: #field_name_lit,
                description: #doc_comment_lit,
                cardinality: dialog_query::attribute::Cardinality::One,
                content_type: #data_type_value,
                marker: std::marker::PhantomData,
            }
        });

        // Generate rule when field conversion - convert Term<T> to Term<Value>
        let attr_string = format!("{}/{}", namespace, field_name_str);
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
                    .the(#attr_string)
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

        // Generate attribute tuples for Attributes implementation
        attributes_tuples.push(quote! {
            (#field_name_lit, dialog_query::attribute::Attribute {
                namespace: #namespace_lit,
                name: #field_name_lit,
                description: #doc_comment_lit,
                cardinality: dialog_query::attribute::Cardinality::One,
                content_type: #data_type_value,
                marker: std::marker::PhantomData,
            })
        });

        // Generate Relation for IntoIterator implementation
        let attr_string = format!("{}/{}", namespace, field_name_str);
        instance_relations.push(quote! {
            dialog_query::Relation::new(
                #attr_string.parse().expect("Failed to parse attribute"),
                self.this.clone(),
                dialog_query::types::Scalar::as_value(&self.#field_name),
            )
        });
    }

    // Generate type names based on struct name (e.g., Person -> PersonMatch, PersonAssert, etc.)
    let match_name = syn::Ident::new(&format!("{}Match", struct_name), struct_name.span());
    let assert_name = syn::Ident::new(&format!("{}Assert", struct_name), struct_name.span());
    let retract_name = syn::Ident::new(&format!("{}Retract", struct_name), struct_name.span());
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

    let expanded = quote! {
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

        /// Assert pattern for #struct_name - used in rule conclusions
        #[derive(Debug, Clone, PartialEq)]
        pub struct #assert_name {
            #(#assert_fields),*
        }

        /// Retract pattern for #struct_name - used for removing facts
        #[derive(Debug, Clone, PartialEq)]
        pub struct #retract_name {
            #(#retract_fields),*
        }

        /// Attributes pattern for #struct_name - enables fluent query building
        #[derive(Debug, Clone, PartialEq)]
        pub struct #attributes_name {
            pub this: dialog_query::term::Term<dialog_query::artifact::Entity>,
            #(#attributes_fields),*
        }


        // Static attribute definitions
        #(#typed_attributes)*

        /// All attributes as Attribute<Value> for the attributes() method
        pub static #attributes_array_name: &[dialog_query::attribute::Attribute<dialog_query::artifact::Value>] = &[
            #(#value_attributes),*
        ];

        /// Attribute tuples for the Attributes trait implementation
        pub static #attribute_tuples_name: &[(&str, dialog_query::attribute::Attribute<dialog_query::artifact::Value>)] = &[
            #(#attributes_tuples),*
        ];

        /// Const Attributes for this concept (Static variant)
        pub const #attributes_const_name: dialog_query::predicate::concept::Attributes =
            dialog_query::predicate::concept::Attributes::Static(#attribute_tuples_name);

        /// Const operator name for this concept
        pub const #operator_const_name: &str = #namespace_lit;

        // Implement Match trait for the Match struct
        impl dialog_query::concept::Match for #match_name {
            type Concept = #struct_name;
            type Instance = #struct_name;

            fn realize(&self, source: dialog_query::selection::Answer) -> std::result::Result<Self::Instance, dialog_query::QueryError> {
                Ok(#struct_name {
                    this: source.get(&self.this)?,
                    #(#field_names: source.get(&self.#field_names)?),*
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
                let application: dialog_query::application::concept::ConceptApplication = self.into();
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
            type Assert = #assert_name;
            type Retract = #retract_name;

            const CONCEPT: dialog_query::predicate::concept::Concept =
                dialog_query::predicate::concept::Concept::Static {
                    operator: #namespace_lit,
                    attributes: &#attributes_const_name,
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
            fn when(terms: dialog_query::Match<Self>) -> dialog_query::rule::When {
                // Create fact selectors for each attribute with type conversion
                let selectors = vec![
                    #(#rule_when_fields),*
                ];

                // Return When collection with all selectors
                selectors.into()
            }
        }



        // Implement Premises for Match to enable it to be used as a premise
        impl dialog_query::rule::Premises for #match_name {
            type IntoIter = std::vec::IntoIter<dialog_query::Premise>;

            fn premises(self) -> Self::IntoIter {
                #struct_name::when(self).into_iter()
            }
        }

        // Implement Instructions for Assert
        impl dialog_query::concept::Instructions for #assert_name {
            type IntoIter = std::vec::IntoIter<dialog_artifacts::Instruction>;

            fn instructions(self) -> Self::IntoIter {
                // For now, return empty vec as placeholder
                // In real implementation, this would generate Assert instructions
                vec![].into_iter()
            }
        }

        // Implement Instructions for Retract
        impl dialog_query::concept::Instructions for #retract_name {
            type IntoIter = std::vec::IntoIter<dialog_artifacts::Instruction>;

            fn instructions(self) -> Self::IntoIter {
                // For now, return empty vec as placeholder
                // In real implementation, this would generate Retract instructions
                vec![].into_iter()
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
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
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
