//! Procedural macros for generating relation attribute structs and rule definitions

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type, Attribute, Meta, Expr, Lit};

/// Procedural macro to generate attribute structs from an enum definition.
///
/// # Example
/// ```ignore
/// use dialog_query_macros::relation;
/// 
/// #[relation]
/// enum Employee {
///     Name(String),
///     Job(String), 
///     Salary(u32),
///     #[many]
///     Address(String),
/// }
/// 
/// // This generates a module `Employee` containing:
/// // - Name, Job, Salary, Address structs (no prefixes!)
/// // - Each struct implements Attribute trait
/// // - You can use Employee::Name::new("John"), etc.
/// ```
#[proc_macro_attribute]
pub fn relation(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    let enum_name = &input.ident;
    
    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => {
            return syn::Error::new_spanned(&input, "relation can only be applied to enums")
                .to_compile_error()
                .into();
        }
    };
    
    let mut structs = Vec::new();
    
    for variant in variants {
        let variant_name = &variant.ident;
        
        // Check if variant has #[many] attribute
        let has_many = variant.attrs.iter().any(|attr| {
            attr.path().is_ident("many")
        });
        
        let cardinality = if has_many {
            quote! { dialog_query::Cardinality::Many }
        } else {
            quote! { dialog_query::Cardinality::One }
        };
        
        // Get the type from the variant
        let field_type = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => {
                return syn::Error::new_spanned(variant, "Variants must have exactly one unnamed field")
                    .to_compile_error()
                    .into();
            }
        };
        
        let value_data_type = type_to_value_data_type(field_type);
        
        // Convert enum and variant names for the attribute name
        // Enum uses dots, variant uses underscores: SimpleRelation::Name -> simple.relation/name
        let enum_dotted = to_snake_case(&enum_name.to_string());
        let variant_snake = to_snake_case_with_underscores(&variant_name.to_string());
        let attribute_name = format!("{}/{}", enum_dotted, variant_snake);
        let attribute_name_lit = syn::LitStr::new(&attribute_name, proc_macro2::Span::call_site());
        
        
        let struct_def = quote! {
            #[doc = concat!("Attribute struct for ", stringify!(#variant_name))]
            pub struct #variant_name(pub #field_type);
            
            impl dialog_query::Attribute for #variant_name {
                fn name() -> &'static str {
                    #attribute_name_lit
                }
                
                fn cardinality() -> dialog_query::Cardinality {
                    #cardinality
                }
                
                fn value_type() -> dialog_query::artifact::ValueDataType {
                    #value_data_type
                }
            }
            
            impl #variant_name {
                #[doc = concat!("Create a new ", stringify!(#variant_name), " attribute")]
                pub fn new(value: impl Into<#field_type>) -> Self {
                    Self(value.into())
                }
                
                /// Get the inner value
                pub fn value(&self) -> &#field_type {
                    &self.0
                }
                
                /// Consume the attribute and return the inner value
                pub fn into_value(self) -> #field_type {
                    self.0
                }
            }

        };
        
        structs.push(struct_def);
    }
    
    let expanded = quote! {
        // Replace the original enum with a module containing the generated structs
        #[allow(non_snake_case)]
        pub mod #enum_name {
            use super::*;
            use dialog_query;
            
            #(#structs)*
        }
    };
    
    TokenStream::from(expanded)
}

fn type_to_value_data_type(ty: &Type) -> proc_macro2::TokenStream {
    let type_str = quote!(#ty).to_string().replace(" ", "");
    
    match type_str.as_str() {
        "String" => quote! { dialog_query::artifact::ValueDataType::String },
        "&str" | "str" => quote! { dialog_query::artifact::ValueDataType::String },
        "bool" => quote! { dialog_query::artifact::ValueDataType::Boolean },
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" => quote! { dialog_query::artifact::ValueDataType::UnsignedInt },
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" => quote! { dialog_query::artifact::ValueDataType::SignedInt },
        "f32" | "f64" => quote! { dialog_query::artifact::ValueDataType::Float },
        "Vec<u8>" => quote! { dialog_query::artifact::ValueDataType::Bytes },
        "dialog_artifacts::Entity" | "Entity" => quote! { dialog_query::artifact::ValueDataType::Entity },
        "dialog_artifacts::Attribute" | "Attribute" => quote! { dialog_query::artifact::ValueDataType::Symbol },
        _ => {
            // For unknown types, default to String to avoid compile-time errors
            // This matches the behavior of the original unwrap_or(ValueDataType::String)
            quote! { dialog_query::artifact::ValueDataType::String }
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

fn to_snake_case_with_underscores(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    
    while let Some(ch) = chars.next() {
        if ch.is_uppercase() {
            if !result.is_empty() {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    
    result
}

/// Derive macro to generate Rule implementation from a struct definition.
///
/// This macro generates all the necessary boilerplate for implementing a rule,
/// including Match, Assert, Retract, and Attributes types.
///
/// # Example
/// ```ignore
/// use dialog_query_macros::Rule;
/// use dialog_query::concept::Concept;
/// use dialog_query::Statements;
/// 
/// #[derive(Rule, Debug, Clone)]
/// pub struct Person {
///     /// Name of the person
///     pub name: String,
///     /// Birthday of the person  
///     pub birthday: u32,
/// }
/// 
/// // This generates:
/// // - person::Match struct for querying
/// // - person::Assert struct for conclusions
/// // - person::Retract struct for retractions  
/// // - person::Attributes struct for fluent queries
/// // - Concept and Rule trait implementations
/// 
/// // Now you can use the generated types:
/// let person_entity = dialog_query::Term::var("person");
/// let attributes = Person::r#match(person_entity.clone());
/// assert_eq!(Person::name(), "person");
/// ```
#[proc_macro_derive(Rule)]
pub fn derive_rule(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    let struct_name = &input.ident;
    let module_name_ident = syn::Ident::new(&to_snake_case(&struct_name.to_string()), struct_name.span());
    
    // Extract fields from the struct
    let fields = match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields_named) => &fields_named.named,
                _ => {
                    return syn::Error::new_spanned(&input, "Rule can only be derived for structs with named fields")
                        .to_compile_error()
                        .into();
                }
            }
        }
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

    // Generate namespace from struct name (e.g., Person -> "person")
    let namespace = to_snake_case(&struct_name.to_string());
    let namespace_lit = syn::LitStr::new(&namespace, proc_macro2::Span::call_site());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_name_str = field_name.to_string();
        let field_name_lit = syn::LitStr::new(&field_name_str, proc_macro2::Span::call_site());
        let field_name_upper = syn::Ident::new(&format!("{}_ATTR", field_name_str.to_uppercase()), field_name.span());
        
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
        attribute_init_fields.push(quote! {
            #field_name: #module_name_ident::#field_name_upper.of(entity.clone())
        });

        // Get the compile-time data type for this field
        let data_type_value = type_to_value_data_type(field_type);
        
        // Generate static typed attribute definitions
        typed_attributes.push(quote! {
            /// Static attribute definition for #field_name
            pub static #field_name_upper: dialog_query::attribute::Attribute<#field_type> = dialog_query::attribute::Attribute {
                namespace: NAMESPACE,
                name: #field_name_lit,
                description: #doc_comment_lit,
                cardinality: dialog_query::attribute::Cardinality::One,
                data_type: #data_type_value,
                marker: std::marker::PhantomData,
            };
        });

        // Generate Attribute<Value> for the attributes() method
        value_attributes.push(quote! {
            dialog_query::attribute::Attribute {
                namespace: NAMESPACE,
                name: #field_name_lit,
                description: #doc_comment_lit,
                cardinality: dialog_query::attribute::Cardinality::One,
                data_type: #data_type_value,
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
                        _type: Default::default(),
                    },
                    dialog_query::term::Term::Constant(value) => dialog_query::term::Term::Constant(dialog_query::types::Scalar::as_value(value)),
                };
                
                dialog_query::fact_selector::FactSelector::<dialog_query::artifact::Value> {
                    the: Some(dialog_query::term::Term::from(#attr_string.parse::<dialog_artifacts::Attribute>().unwrap())),
                    of: Some(terms.this.clone()),
                    is: Some(value_term),
                    fact: None,
                }
            }
        });
    }

    // Generate type names based on struct name (e.g., Person -> PersonMatch, PersonAssert, etc.)
    let match_name = syn::Ident::new(&format!("{}Match", struct_name), struct_name.span());
    let assert_name = syn::Ident::new(&format!("{}Assert", struct_name), struct_name.span());
    let retract_name = syn::Ident::new(&format!("{}Retract", struct_name), struct_name.span());
    let attributes_name = syn::Ident::new(&format!("{}Attributes", struct_name), struct_name.span());

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

        // Module to hold #struct_name-related constants and attributes
        pub mod #module_name_ident {
            use super::*;
            use dialog_query::attribute::{Attribute, Cardinality, Match};
            use dialog_query::artifact::{Entity, Value};
            use dialog_query::concept::{Concept, Instructions};
            use dialog_query::fact_selector::FactSelector;
            use dialog_query::rule::{Rule, When};
            use dialog_query::statement::Statement;
            use dialog_query::term::Term;
            use dialog_query::types::Scalar;
            use dialog_query::Statements;
            use std::marker::PhantomData;

            /// The namespace for #struct_name attributes
            pub const NAMESPACE: &str = #namespace_lit;
            
            // Static attribute definitions
            #(#typed_attributes)*
            
            /// All attributes as Attribute<Value> for the attributes() method
            pub static ATTRIBUTES: &[dialog_query::attribute::Attribute<dialog_query::artifact::Value>] = &[
                #(#value_attributes),*
            ];
        }

        // Implement Concept trait
        impl dialog_query::concept::Concept for #struct_name {
            type Match = #match_name;
            type Assert = #assert_name;
            type Retract = #retract_name;  
            type Attributes = #attributes_name;

            fn name() -> &'static str {
                #namespace_lit
            }

            fn attributes() -> &'static [dialog_query::attribute::Attribute<dialog_query::artifact::Value>] {
                #module_name_ident::ATTRIBUTES
            }

            fn r#match<T: Into<dialog_query::term::Term<dialog_query::artifact::Entity>>>(this: T) -> Self::Attributes {
                let entity = this.into();
                #attributes_name {
                    this: entity.clone(),
                    #(#attribute_init_fields),*
                }
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
            pub async fn query<S: dialog_query::artifact::ArtifactStore + Clone + Send + 'static>(
                &self,
                store: S,
            ) -> dialog_query::error::QueryResult<Vec<#struct_name>> {
                use dialog_query::syntax::{VariableScope, Syntax};
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
                let context = EvaluationContext::new(store.clone(), initial_selection);
                
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

        // Implement Statements for Match to enable it to be used as a premise
        impl dialog_query::Statements for #match_name {
            type IntoIter = std::vec::IntoIter<dialog_query::statement::Statement>;
            
            fn statements(self) -> Self::IntoIter {
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