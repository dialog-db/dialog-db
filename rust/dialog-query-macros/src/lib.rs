//! Procedural macros for generating relation attribute structs

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type};

/// Procedural macro to generate attribute structs from an enum definition.
///
/// # Example
/// ```
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
        
        let struct_def = quote! {
            #[doc = concat!("Attribute struct for ", stringify!(#variant_name))]
            pub struct #variant_name(pub #field_type);
            
            impl dialog_query::Attribute for #variant_name {
                fn name() -> &'static str {
                    #attribute_name
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
        #[allow(non_snake_case)]
        pub mod #enum_name {
            use super::*;
            
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