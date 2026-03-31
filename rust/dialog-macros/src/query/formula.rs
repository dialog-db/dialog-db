//! Formula derive macro implementation
//!
//! Generates a computed attribute system from a struct. A formula has
//! "input" fields (provided by the caller) and "output" fields (computed by
//! the formula's `compute()` function).
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
//!     #[output]             // marks this as an output field (cost defaults to 1)
//!     pub full: String,
//! }
//!
//! // User must implement the compute function:
//! impl FullNameFormula {
//!     fn compute(input: FullNameFormulaInput) -> Vec<Self> {
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
//! // -- Input struct (non-output fields only) --
//! pub struct FullNameFormulaInput {
//!     pub first: String,
//!     pub last: String,
//! }
//!
//! // -- Query struct (all fields as Term<T> for query patterns) --
//! pub struct FullNameFormulaQuery {
//!     pub first: Term<String>,
//!     pub last: Term<String>,
//!     pub full: Term<String>,
//! }
//!
//! // -- Cells definition (schema for the formula's inputs/outputs) --
//! // Static OnceLock holding cell definitions built via a builder:
//! //   builder.cell("first", DataType::String).the("The first name").required();
//! //   builder.cell("last",  DataType::String).the("The last name").required();
//! //   builder.cell("full",  DataType::String).the("The computed full name").output(1);
//!
//! // -- Formula trait impl --
//! impl Formula for FullNameFormula {
//!     type Input = FullNameFormulaInput;
//!
//!     fn cells() -> &'static Cells { /* lazily built from above */ }
//!     fn cost() -> usize { 1 }  // sum of all #[output(cost)] values
//!     fn compute(input: Self::Input) -> Vec<Self> { /* delegates to user impl */ }
//! }
//!
//! // -- Conversions --
//! // FullNameFormulaQuery → Parameters
//! // TryFrom<&mut Bindings> for FullNameFormulaInput (reads input cells from bindings)
//! // Formula::write() for FullNameFormula (writes output cells to bindings)
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{extract_doc_comments, parse_output_attribute, type_to_value_data_type};

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

    // Partition fields into "input" (required by caller) and "output" (computed).
    // Fields marked with #[output] or #[output(cost = N)] become outputs.
    let mut input_fields = Vec::new();
    let mut output_fields = Vec::new(); // (name, type, doc, cost)
    let mut all_fields = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        // Extract doc comment
        let doc_comment = extract_doc_comments(&field.attrs);

        // Check if field has #[output] and parse optional cost
        let output_info = match parse_output_attribute(&field.attrs) {
            Ok(info) => info,
            Err(e) => return e.to_compile_error().into(),
        };

        if let Some(cost) = output_info {
            all_fields.push((field_name, field_type, doc_comment.clone(), true, cost));
            output_fields.push((field_name, field_type, doc_comment, cost));
        } else {
            all_fields.push((field_name, field_type, doc_comment.clone(), false, 0));
            input_fields.push((field_name, field_type, doc_comment));
        }
    }

    // Validate at least one output field exists
    if output_fields.is_empty() {
        return syn::Error::new_spanned(
            &input,
            "Formula must have at least one field marked with #[output]",
        )
        .to_compile_error()
        .into();
    }

    // Total cost is the sum of per-field costs. This lets the query planner
    // estimate how expensive it is to evaluate this formula.
    let total_cost: usize = output_fields.iter().map(|(_, _, _, cost)| cost).sum();

    // Generate type names
    let input_name = syn::Ident::new(&format!("{}Input", struct_name), struct_name.span());
    let query_name = syn::Ident::new(&format!("{}Query", struct_name), struct_name.span());
    let cells_name = syn::Ident::new(
        &format!("{}_CELLS", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate Input struct fields (only non-output fields)
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

    // Generate Query struct fields — each field becomes Term<T> for pattern matching
    let query_struct_fields: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, doc, _is_output, _cost)| {
            let doc_lit = syn::LitStr::new(doc, proc_macro2::Span::call_site());
            quote! {
                #[doc = #doc_lit]
                pub #name: dialog_query::Term<#ty>
            }
        })
        .collect();

    // Generate cell definitions that describe the formula's schema.
    // Input fields are marked `.required()`, output fields are marked `.output(cost)`.
    let cell_definitions: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, doc, is_output, cost)| {
            let name_str = name.to_string();
            let name_lit = syn::LitStr::new(&name_str, proc_macro2::Span::call_site());
            let doc_lit = syn::LitStr::new(doc, proc_macro2::Span::call_site());
            let data_type = type_to_value_data_type(ty);

            if *is_output {
                quote! {
                    builder
                        .cell(#name_lit, #data_type)
                        .the(#doc_lit)
                        .output(#cost);
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

    // Generate field names for Query Into<Parameters>
    let all_field_names: Vec<_> = all_fields.iter().map(|(name, _, _, _, _)| name).collect();
    let all_field_name_lits: Vec<_> = all_fields
        .iter()
        .map(|(name, _, _, _, _)| {
            let name_str = name.to_string();
            syn::LitStr::new(&name_str, proc_macro2::Span::call_site())
        })
        .collect();

    // Generate Formula::write() statements — only output fields are written
    // back to the bindings after computation.
    let write_statements: Vec<_> = output_fields
        .iter()
        .map(|(name, _ty, _, _cost)| {
            let name_str = name.to_string();
            let name_lit = syn::LitStr::new(&name_str, proc_macro2::Span::call_site());
            quote! {
                bindings.write(#name_lit, &self.#name.clone().into())?;
            }
        })
        .collect();

    // Generate per-field realize expressions.
    // Each field resolves the term to a Value, then converts to the field type.
    let realize_fields: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, _, _, _)| {
            quote! {
                #name: #ty::try_from(source.lookup(&dialog_query::Term::from(&self.#name))?)?
            }
        })
        .collect();

    let expanded = quote! {
            /// Input structure for #struct_name formula
            ///
            /// Contains only the required (non-output) fields that must be provided
            /// to compute the formula.
            #[derive(Debug, Clone)]
            pub struct #input_name {
                #(#input_struct_fields),*
            }

            /// Query pattern for #struct_name formula.
            ///
            /// Contains all fields (both input and output) as Term<T> for pattern matching.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct #query_name {
                #(#query_struct_fields),*
            }

            /// Static storage for formula cells
            static #cells_name: ::std::sync::OnceLock<dialog_query::Cells> = ::std::sync::OnceLock::new();

            impl dialog_query::Predicate for #struct_name {
                type Conclusion = #struct_name;
                type Application = #query_name;
                type Descriptor = dialog_query::Entity;
            }

            impl dialog_query::Application for #query_name
    {
                type Conclusion = #struct_name;

                fn evaluate<'__a, __Env, __M: dialog_query::Selection + '__a>(
                    self,
                    selection: __M,
                    _source: &'__a dialog_query::source::Source<'__a, __Env>,
                ) -> impl dialog_query::Selection + '__a
                where
                    __Env: dialog_query::Provider<dialog_query::archive::Get>
                        + dialog_query::Provider<dialog_query::archive::Put>
                        + dialog_query::ConditionalSync
                        + 'static,
                {
                    let formula: dialog_query::FormulaQuery = self.into();
                    formula.evaluate(selection)
                }

                fn realize(&self, source: dialog_query::Match) -> std::result::Result<Self::Conclusion, dialog_query::EvaluationError> {
                    Ok(#struct_name {
                        #(#realize_fields),*
                    })
                }
            }

            impl ::std::convert::From<#query_name> for dialog_query::Parameters {
                fn from(terms: #query_name) -> Self {
                    let mut parameters = Self::new();
                    #(parameters.insert(#all_field_name_lits.into(), dialog_query::Term::<dialog_query::types::Any>::from(terms.#all_field_names));)*
                    parameters
                }
            }

            impl From<#query_name> for dialog_query::Premise
    {
                fn from(source: #query_name) -> Self {
                    let formula: dialog_query::FormulaQuery = source.into();
                    dialog_query::Premise::Assert(dialog_query::Proposition::from(formula))
                }
            }

            impl From<#query_name> for dialog_query::Proposition
    {
                fn from(source: #query_name) -> Self {
                    let formula: dialog_query::FormulaQuery = source.into();
                    dialog_query::Proposition::from(formula)
                }
            }

            impl ::std::ops::Not for #query_name
    {
                type Output = dialog_query::Premise;

                fn not(self) -> Self::Output {
                    let proposition: dialog_query::Proposition = self.into();
                    dialog_query::Premise::Unless(dialog_query::Negation(proposition))
                }
            }

            impl ::std::convert::TryFrom<&mut dialog_query::Bindings> for #input_name {
                type Error = dialog_query::EvaluationError;

                fn try_from(bindings: &mut dialog_query::Bindings) -> ::std::result::Result<Self, Self::Error> {
                    Ok(#input_name {
                        #(#input_field_names: bindings.resolve(#input_field_name_lits)?.try_into()?),*
                    })
                }
            }

            impl dialog_query::Formula for #struct_name {
                type Input = #input_name;

                fn cells() -> &'static dialog_query::Cells {
                    #cells_name.get_or_init(|| {
                        dialog_query::Cells::define(|builder| {
                            #(#cell_definitions)*
                        })
                    })
                }

                fn cost() -> usize {
                    #total_cost
                }

                fn apply(terms: dialog_query::Parameters) -> ::std::result::Result<#query_name, dialog_query::error::TypeError> {
                    let cells = <#struct_name as dialog_query::Formula>::cells();
                    let conformed = cells.conform(terms)?;
                    ::std::result::Result::Ok(#query_name {
                        #(#all_field_names: conformed
                            .get(#all_field_name_lits)
                            .cloned()
                            .unwrap_or_else(|| dialog_query::Term::<dialog_query::types::Any>::blank())
                            .narrow()
                            .map_err(|e| e.at(#all_field_name_lits.into()))?
                        ),*
                    })
                }

                fn compute(input: Self::Input) -> ::std::vec::Vec<Self> {
                    #struct_name::compute(input)
                }

                fn write(&self, bindings: &mut dialog_query::Bindings) -> ::std::result::Result<(), dialog_query::EvaluationError> {
                    #(#write_statements)*
                    ::std::result::Result::Ok(())
                }
            }
        };

    TokenStream::from(expanded)
}
