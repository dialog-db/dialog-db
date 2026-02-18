//! Formula derive macro implementation
//!
//! Generates a computed/derived attribute system from a struct. A formula has
//! "input" fields (provided by the caller) and "derived" fields (computed by
//! the formula's `derive()` function).
//!
//! # Example input
//!
//! ```rust,ignore
//! /// Computes a person's full name from parts
//! #[derive(Formula)]
//! pub struct FullNameFormula {
//!     /// The first name
//!     pub first: String,
//!     /// The last name
//!     pub last: String,
//!     /// The computed full name
//!     #[derived]            // marks this as an output field (cost defaults to 0)
//!     pub full: String,
//! }
//!
//! // User must implement the derive function:
//! impl FullNameFormula {
//!     fn derive(input: FullNameFormulaInput) -> Vec<Self> {
//!         vec![FullNameFormula {
//!             first: input.first.clone(),
//!             last: input.last.clone(),
//!             full: format!("{} {}", input.first, input.last),
//!         }]
//!     }
//! }
//! ```
//!
//! # Generated output (simplified)
//!
//! ```rust,ignore
//! // -- Input struct (non-derived fields only) --
//! pub struct FullNameFormulaInput {
//!     pub first: String,
//!     pub last: String,
//! }
//!
//! // -- Match struct (all fields as Term<T> for query patterns) --
//! pub struct FullNameFormulaMatch {
//!     pub first: Term<String>,
//!     pub last: Term<String>,
//!     pub full: Term<String>,
//! }
//!
//! // -- Cells definition (schema for the formula's inputs/outputs) --
//! // Static OnceLock holding cell definitions built via a builder:
//! //   builder.cell("first", DataType::String).the("The first name").required();
//! //   builder.cell("last",  DataType::String).the("The last name").required();
//! //   builder.cell("full",  DataType::String).the("The computed full name").derived(0);
//!
//! // -- Formula trait impl --
//! impl Formula for FullNameFormula {
//!     type Input = FullNameFormulaInput;
//!     type Match = FullNameFormulaMatch;
//!
//!     fn operator() -> &'static str { "full-name-formula" }
//!     fn cells() -> &'static Cells { /* lazily built from above */ }
//!     fn cost() -> usize { 0 }  // sum of all #[derived(cost)] values
//!     fn derive(input: Self::Input) -> Vec<Self> { /* delegates to user impl */ }
//! }
//!
//! // -- Conversions --
//! // FullNameFormulaMatch → Parameters
//! // TryFrom<&mut Cursor> for FullNameFormulaInput (reads input cells from cursor)
//! // Output::write() for FullNameFormula (writes derived cells to cursor)
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{
    extract_doc_comments, parse_derived_attribute, to_snake_case, type_to_value_data_type,
};

pub fn derive(input: TokenStream) -> TokenStream {
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

    // Partition fields into "input" (required by caller) and "derived" (computed).
    // Fields marked with #[derived] or #[derived(cost = N)] become outputs.
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

    // Total cost is the sum of per-field costs. This lets the query planner
    // estimate how expensive it is to evaluate this formula.
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

    // Generate cell definitions that describe the formula's schema.
    // Input fields are marked `.required()`, derived fields are marked `.derived(cost)`.
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

    // Generate Output::write() statements — only derived fields are written
    // back to the cursor after computation.
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

            fn derive(input: Self::Input) -> ::std::vec::Vec<Self> {
                #struct_name::derive(input)
            }
        }
    };

    TokenStream::from(expanded)
}
