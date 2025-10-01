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

    // Extract fields from the struct
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
    let mut match_term_conversions = Vec::new();
    let mut attributes_tuples = Vec::new();

    // Generate namespace from struct name (e.g., Person -> "person")
    let namespace = to_snake_case(&struct_name.to_string());
    let namespace_lit = syn::LitStr::new(&namespace, proc_macro2::Span::call_site());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_name_str = field_name.to_string();
        let field_name_lit = syn::LitStr::new(&field_name_str, proc_macro2::Span::call_site());

        // Store field name for later use in reconstruction
        field_names.push(field_name);

        // Extract doc comment for the field
        let doc_comment = extract_doc_comments(&field.attrs);
        let doc_comment_lit = syn::LitStr::new(&doc_comment, proc_macro2::Span::call_site());

        // Generate Match field (Term<T>)
        match_fields.push(quote! {
            /// #doc_comment
            pub #field_name: dialog_query::term::Term<#field_type>
        });

        // Generate Attributes field (Match<T>)
        attributes_fields.push(quote! {
            /// #doc_comment
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
        &format!("{}_ATTRIBUTES", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let attribute_tuples_name = syn::Ident::new(
        &format!(
            "{}_ATTRIBUTE_TUPLES",
            struct_name.to_string().to_uppercase()
        ),
        struct_name.span(),
    );

    let expanded = quote! {
        /// Match pattern for #struct_name - has Term-wrapped fields for querying
        #[derive(Debug, Clone)]
        pub struct #match_name {
            /// The entity being matched
            pub this: dialog_query::term::Term<dialog_query::artifact::Entity>,
            #(#match_fields),*
        }

        /// Assert pattern for #struct_name - used in rule conclusions
        #[derive(Debug, Clone)]
        pub struct #assert_name {
            #(#assert_fields),*
        }

        /// Retract pattern for #struct_name - used for removing facts
        #[derive(Debug, Clone)]
        pub struct #retract_name {
            #(#retract_fields),*
        }

        /// Attributes pattern for #struct_name - enables fluent query building
        #[derive(Debug, Clone)]
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

        // Implement Match trait for the Match struct
        impl dialog_query::concept::Match for #match_name {
            type Instance = #struct_name;
            type Attributes = #attributes_name;

            fn term_for(&self, name: &str) -> Option<&dialog_query::term::Term<dialog_query::artifact::Value>> {
                match name {
                    #(#match_term_conversions),*
                    _ => None
                }
            }

            fn this(&self) -> dialog_query::term::Term<dialog_query::artifact::Entity> {
                self.this.clone()
            }
        }

        // Implement Attributes trait
        impl dialog_query::concept::Attributes for #attributes_name {
            fn attributes() -> &'static [(&'static str, dialog_query::attribute::Attribute<dialog_query::artifact::Value>)] {
                #attribute_tuples_name
            }

            fn of<T: Into<dialog_query::term::Term<dialog_query::artifact::Entity>>>(entity: T) -> Self {
                let entity = entity.into();
                #attributes_name {
                    this: entity.clone(),
                    #(#attribute_init_fields),*
                }
            }
        }

        // Implement Instance trait for the concept struct
        impl dialog_query::concept::Instance for #struct_name {
            fn this(&self) -> dialog_query::artifact::Entity {
                // For now, we'll panic as we don't have an entity field on the struct
                // In a real implementation, you might want to add an entity field to the struct
                panic!("Instance trait implementation requires an entity field on the struct")
            }
        }

        // Implement Concept trait
        impl dialog_query::concept::Concept for #struct_name {
            type Instance = #struct_name;
            type Match = #match_name;
            type Assert = #assert_name;
            type Retract = #retract_name;
            type Attributes = #attributes_name;

            fn name() -> &'static str {
                #namespace_lit
            }
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

        // Implement query helper method for Match structs
        impl #match_name {
            /// Query the store for concept instances matching this pattern
            ///
            /// This is a convenience method that executes the query plan and converts
            /// MatchFrames back to concept instances.
            pub async fn query<S: dialog_query::query::Source>(
                &self,
                store: S,
            ) -> dialog_query::error::QueryResult<Vec<#struct_name>> {
                use dialog_query::syntax::VariableScope;
                use dialog_query::plan::{EvaluationContext, EvaluationPlan};
                use dialog_query::selection::Match;
                use dialog_query::premise::Premise;
                use futures_util::{stream, StreamExt, TryStreamExt};
                use dialog_query::term::Term;

                // Create execution plan
                let scope = VariableScope::new();
                let rule_when = #struct_name::when(self.clone());
                let plans: Vec<_> = rule_when.into_iter().map(|stmt| stmt.plan(&scope)).collect::<Result<Vec<_>, _>>()?;

                // For simplicity, we'll execute each plan sequentially and combine results
                // In a real implementation, this could be optimized with proper join logic
                if plans.is_empty() {
                    return Ok(vec![]);
                }

                // Start with an empty match frame
                let initial_match = Match::new();
                let initial_selection = stream::iter(vec![Ok(initial_match)]);
                let context = EvaluationContext::single(store.clone(), initial_selection, VariableScope::new());

                // For now, we'll just execute the first plan to demonstrate the pattern
                // In a complete implementation, we'd need to handle plan joining properly
                let selection = if let Some(first_plan) = plans.into_iter().next() {
                    first_plan.evaluate(context)
                } else {
                    return Ok(vec![]);
                };

                // Collect all match frames
                let match_frames: Vec<Match> = selection.try_collect().await?;

                // Convert match frames back to concept instances
                let mut results = Vec::new();
                for frame in match_frames {
                    // Extract the entity (this field)
                    let entity = frame.get(&self.this)?;

                    // Extract each field from the frame and construct the concept instance
                    let instance = #struct_name {
                        #(#field_names: frame.get(&self.#field_names)?),*
                    };

                    results.push(instance);
                }

                Ok(results)
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
        "Vec<u8>" => quote! { dialog_query::artifact::Type::Bytes },
        "dialog_artifacts::Entity" | "Entity" => {
            quote! { dialog_query::artifact::Type::Entity }
        }
        "dialog_artifacts::Attribute" | "Attribute" => {
            quote! { dialog_query::artifact::Type::Symbol }
        }
        _ => {
            // For unknown types, default to String to avoid compile-time errors
            // This matches the behavior of the original unwrap_or(ValueDataType::String)
            quote! { dialog_query::artifact::Type::String }
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
