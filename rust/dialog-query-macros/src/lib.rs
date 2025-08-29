//! Procedural macros for generating relation attribute structs and rule definitions

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type};

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
                
                fn value_type() -> dialog_query::ValueDataType {
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
        "String" => quote! { dialog_query::ValueDataType::String },
        "&str" | "str" => quote! { dialog_query::ValueDataType::String },
        "bool" => quote! { dialog_query::ValueDataType::Boolean },
        "u8" | "u16" | "u32" | "u64" | "u128" => quote! { dialog_query::ValueDataType::UnsignedInt },
        "i8" | "i16" | "i32" | "i64" | "i128" => quote! { dialog_query::ValueDataType::SignedInt },
        "f32" | "f64" => quote! { dialog_query::ValueDataType::Float },
        "Vec<u8>" => quote! { dialog_query::ValueDataType::Bytes },
        "dialog_artifacts::Entity" => quote! { dialog_query::ValueDataType::Entity },
        "dialog_artifacts::Attribute" => quote! { dialog_query::ValueDataType::Symbol },
        _ => {
            let error_msg = format!("Unsupported type for attribute: {}", type_str);
            quote! { compile_error!(#error_msg) }
        }
    }
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
    let mut statements_impl = Vec::new();
    let mut default_match_fields = Vec::new();
    let mut attribute_init_fields = Vec::new();

    // Generate namespace from struct name (e.g., Person -> "person")
    let namespace = to_snake_case(&struct_name.to_string());
    let namespace_lit = syn::LitStr::new(&namespace, proc_macro2::Span::call_site());

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_name_str = field_name.to_string();
        let field_name_lit = syn::LitStr::new(&field_name_str, proc_macro2::Span::call_site());
        
        // Generate Match field (Term<T>)
        match_fields.push(quote! {
            pub #field_name: dialog_query::Term<#field_type>
        });

        // Generate Attributes field (Match<T>)  
        attributes_fields.push(quote! {
            pub #field_name: dialog_query::attribute::Match<#field_type>
        });

        // Generate Assert field (Term<T>)
        assert_fields.push(quote! {
            pub #field_name: dialog_query::Term<#field_type>
        });

        // Generate Retract field (Term<T>)
        retract_fields.push(quote! {
            pub #field_name: dialog_query::Term<#field_type>
        });

        // Generate statement for Statements implementation
        let attr_string = format!("{}/{}", namespace, field_name_str);
        statements_impl.push(quote! {
            dialog_query::Statement::fact(
                Some(dialog_query::Term::from(#attr_string.parse::<dialog_query::artifact::Attribute>().unwrap())),
                Some(self.this.clone()),
                Some(dialog_query::Term::<dialog_query::artifact::Value>::var(#field_name_lit))
            )
        });

        // Generate default field for Match::default()
        default_match_fields.push(quote! {
            #field_name: dialog_query::Term::var(#field_name_lit)
        });

        // Generate attribute initialization for Attributes
        attribute_init_fields.push(quote! {
            #field_name: dialog_query::attribute::Match::new(
                #namespace_lit,
                #field_name_lit, 
                "",  // description - TODO: extract from docstring
                entity_term.clone()
            )
        });
    }

    let expanded = quote! {
        // Generate the module containing all the rule-related types
        pub mod #module_name_ident {
            use super::*;
            use dialog_query;

            pub const NAMESPACE: &'static str = #namespace_lit;

            /// Pattern for matching this concept in rule conditions
            #[derive(Debug, Clone)]
            pub struct Match {
                pub this: dialog_query::Term<dialog_query::Entity>,
                #(#match_fields),*
            }

            impl dialog_query::Statements for Match {
                type IntoIter = std::vec::IntoIter<dialog_query::Statement>;
                
                fn statements(self) -> Self::IntoIter {
                    vec![
                        #(#statements_impl),*
                    ].into_iter()
                }
            }

            impl Default for Match {
                fn default() -> Self {
                    Self {
                        this: dialog_query::Term::var("this"),
                        #(#default_match_fields),*
                    }
                }
            }

            impl From<Match> for dialog_query::Term<dialog_query::Entity> {
                fn from(source: Match) -> Self {
                    source.this
                }
            }

            /// Pattern for asserting this concept in rule conclusions  
            #[derive(Debug, Clone)]
            pub struct Assert {
                pub this: dialog_query::Term<dialog_query::Entity>,
                #(#assert_fields),*
            }

            /// Pattern for retracting this concept in rule conclusions
            #[derive(Debug, Clone)]
            pub struct Retract {
                pub this: dialog_query::Term<dialog_query::Entity>,
                #(#retract_fields),*
            }

            /// Attributes for fluent query building
            #[derive(Debug, Clone)]
            pub struct Attributes {
                #(#attributes_fields),*
            }
        }

        // Implement Concept trait
        impl dialog_query::concept::Concept for #struct_name {
            type Match = #module_name_ident::Match;
            type Assert = #module_name_ident::Assert;
            type Retract = #module_name_ident::Retract;  
            type Attributes = #module_name_ident::Attributes;

            fn name() -> &'static str {
                #namespace_lit
            }

            fn r#match<T: Into<dialog_query::Term<dialog_query::Entity>>>(this: T) -> Self::Attributes {
                let entity_term: dialog_query::Term<dialog_query::Entity> = this.into();
                Self::Attributes {
                    #(#attribute_init_fields),*
                }
            }
        }

        // Implement Rule trait  
        impl dialog_query::Rule for #struct_name {
            fn when(terms: Self::Match) -> dialog_query::When {
                // Default rule: convert statements to When
                let statements: Vec<dialog_query::Statement> = terms.statements().collect();
                statements.into()
            }
        }

        // Generate constructor function
        #[allow(non_snake_case)]
        pub fn #struct_name<T: Into<dialog_query::Term<dialog_query::Entity>>>(this: T) -> #module_name_ident::Attributes {
            #struct_name::r#match(this)
        }
    };

    TokenStream::from(expanded)
}