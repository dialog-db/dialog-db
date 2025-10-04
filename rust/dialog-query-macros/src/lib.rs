//! Procedural macros for generating rule definitions

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Expr, Fields, Lit, Meta, Type};

/// Derive macro to generate Rule implementation from a struct definition.
///
/// This macro generates all the necessary boilerplate for implementing a rule,
/// including Match, Assert, Retract, and Attributes types.
///
/// # Example
///
/// This macro transforms input like:
/// ```text
/// use dialog_query::concept::Concept;
/// use dialog_query::rule::Rule as RuleTrait;
/// use dialog_query::Term;
/// use dialog_query_macros::Rule;
///
/// #[derive(Rule, Debug, Clone)]
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
#[proc_macro_derive(Rule)]
pub fn derive_rule(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Extract fields from the ostruct
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "Rule can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "Rule can only be derived for structs")
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
                    dialog_query::Term::<#field_type>::var("test")
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

        // Implement ConceptType trait for the Match struct
        impl dialog_query::predicate::concept::ConceptType for #match_name {
            fn operator() -> &'static str {
                #namespace_lit
            }

            fn attributes() -> &'static dialog_query::predicate::concept::Attributes {
                &#attributes_const_name
            }
        }

        // Implement Match trait for the Match struct
        impl dialog_query::concept::Match for #match_name {
            type Concept = #struct_name;
            type Instance = #struct_name;
        }

        // Add inherent query method so users don't need to import Match trait
        impl #match_name {
            /// Query for instances matching this pattern
            ///
            /// This is a convenience method that delegates to the Match trait's query method
            /// and collects the results. It allows calling query without importing the Match trait.
            pub async fn query<S: dialog_query::query::Source>(
                &self,
                source: S,
            ) -> Result<Vec<#struct_name>, dialog_query::QueryError> {
                use dialog_query::query::Output;
                Output::try_collect(<Self as dialog_query::concept::Match>::query(self, source)).await
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

        // Implement TryFrom<selection::Match> for the concept struct
        // Extracts values from the match by field name
        impl std::convert::TryFrom<dialog_query::selection::Match> for #struct_name {
            type Error = dialog_query::error::InconsistencyError;

            fn try_from(input: dialog_query::selection::Match) -> Result<Self, Self::Error> {
                use dialog_query::artifact::{Type, Value};
                use dialog_query::term::Term;

                Ok(Self {
                    this: input.get(&<Self as dialog_query::concept::Concept>::Term::this())?,
                    #(#field_names: input.get(&<Self as dialog_query::concept::Concept>::Term::#field_names())?),*
                })
            }
        }

        // Implement Instance trait for the concept struct
        impl dialog_query::concept::Instance for #struct_name {
            fn this(&self) -> dialog_query::artifact::Entity {
                self.this.clone()
            }
        }

        // Implement ConceptType trait for the struct
        impl dialog_query::predicate::concept::ConceptType for #struct_name {
            fn operator() -> &'static str {
                #namespace_lit
            }

            fn attributes() -> &'static dialog_query::predicate::concept::Attributes {
                &#attributes_const_name
            }
        }

        // Implement Concept trait
        impl dialog_query::concept::Concept for #struct_name {
            type Instance = #struct_name;
            type Match = #match_name;
            type Term = #terms_name;
            type Assert = #assert_name;
            type Retract = #retract_name;
        }

        // Implement Rule trait
        impl dialog_query::rule::Rule for #struct_name {
            fn when(terms: Self::Match) -> dialog_query::rule::When {
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
/// NOTE: This uses string-based type matching because the generated attributes
/// are in `static` declarations, which require const initialization. Since
/// `IntoValueDataType::into_value_data_type()` is not a const fn, we cannot
/// call it in static context.
///
/// This is a known limitation. Type aliases will not work correctly.
/// For example: `type MyString = String;` will default to `Type::Bytes`.
///
/// Future improvement: Use `LazyLock` for runtime trait-based initialization,
/// or make `IntoValueDataType::into_value_data_type()` a const fn (requires nightly).
fn type_to_value_data_type(ty: &Type) -> proc_macro2::TokenStream {
    let type_str = quote!(#ty).to_string().replace(" ", "");

    match type_str.as_str() {
        "String" => quote! { dialog_query::artifact::Type::String },
        "&str" | "str" => quote! { dialog_query::artifact::Type::String },
        "bool" => quote! { dialog_query::artifact::Type::Boolean },
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" => {
            quote! { dialog_query::artifact::Type::UnsignedInt }
        }
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" => {
            quote! { dialog_query::artifact::Type::SignedInt }
        }
        "f32" | "f64" => quote! { dialog_query::artifact::Type::Float },
        "Vec<u8>" | "Vec<u8>" => quote! { dialog_query::artifact::Type::Bytes },
        "dialog_artifacts::Entity" | "Entity" => {
            quote! { dialog_query::artifact::Type::Entity }
        }
        "dialog_query::Entity" | "crate::Entity" => {
            quote! { dialog_query::artifact::Type::Entity }
        }
        "dialog_artifacts::Attribute" | "Attribute" => {
            quote! { dialog_query::artifact::Type::Symbol }
        }
        "Concept" | "dialog_query::Concept" | "crate::Concept" => {
            // Concept is just an Entity wrapper
            quote! { dialog_query::artifact::Type::Entity }
        }
        _ => {
            // For unknown types, default to Bytes
            // TODO: Emit a compile warning for unknown types
            quote! { dialog_query::artifact::Type::Bytes }
        }
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
