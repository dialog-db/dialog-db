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

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{extract_doc_comments, parse_output_attribute, type_to_value_data_type};

pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // Type-parameter idents: a field whose type is exactly one of
    // these is a *scheme* field — its cell carries the parameter's
    // SchemeBound and a scheme label, and cells sharing a parameter
    // share one type variable at inference time.
    let scheme_params: Vec<syn::Ident> = generics.type_params().map(|p| p.ident.clone()).collect();
    let scheme_param_of = |ty: &syn::Type| -> Option<syn::Ident> {
        if let syn::Type::Path(tp) = ty
            && tp.qself.is_none()
            && tp.path.segments.len() == 1
        {
            let ident = &tp.path.segments[0].ident;
            if tp.path.segments[0].arguments.is_none() && scheme_params.contains(ident) {
                return Some(ident.clone());
            }
        }
        None
    };

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

    // Generate Input struct fields (only non-output fields). Forward the
    // user's field doc when present; otherwise synthesize a fallback so
    // downstream crates with `#![deny(missing_docs)]` still compile.
    let input_struct_fields: Vec<_> = input_fields
        .iter()
        .map(|(name, ty, doc)| {
            let doc_text = if doc.is_empty() {
                format!("The `{name}` input to [`{struct_name}`].")
            } else {
                doc.clone()
            };
            let doc_lit = syn::LitStr::new(&doc_text, proc_macro2::Span::call_site());
            quote! {
                #[doc = #doc_lit]
                pub #name: #ty
            }
        })
        .collect();

    // Generate Query struct fields: each field becomes Term<T> for pattern matching
    let query_struct_fields: Vec<_> = all_fields
        .iter()
        .map(|(name, ty, doc, _is_output, _cost)| {
            let doc_text = if doc.is_empty() {
                format!("Term matching the `{name}` field of [`{struct_name}`].")
            } else {
                doc.clone()
            };
            let doc_lit = syn::LitStr::new(&doc_text, proc_macro2::Span::call_site());
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

            // A scheme field's cell carries the bound of its type
            // parameter and the parameter name as the scheme label;
            // a concrete field's cell carries its singleton kind.
            let (data_type, scheme) = match scheme_param_of(ty) {
                Some(param) => {
                    let label =
                        syn::LitStr::new(&param.to_string(), proc_macro2::Span::call_site());
                    (
                        quote! {
                            Some(dialog_query::type_system::Type::primitive_set(
                                <#param as dialog_query::SchemeBound>::BOUND,
                            ))
                        },
                        Some(quote! { .scheme(#label) }),
                    )
                }
                None => (type_to_value_data_type(ty), None),
            };

            if *is_output {
                quote! {
                    builder
                        .cell(#name_lit, #data_type)
                        .the(#doc_lit)
                        #scheme
                        .output(#cost);
                }
            } else {
                quote! {
                    builder
                        .cell(#name_lit, #data_type)
                        .the(#doc_lit)
                        #scheme
                        .required();
                }
            }
        })
        .collect();

    // The Rust field name is normalized for the parameter surface:
    // `unraw()` drops any `r#` raw-identifier prefix, then
    // `to_case(Case::Kebab)` matches the formal-notation convention
    // used by attribute names elsewhere.

    // Generate field names for Input TryFrom
    let input_field_names: Vec<_> = input_fields.iter().map(|(name, _, _)| name).collect();
    let input_field_name_lits: Vec<_> = input_fields
        .iter()
        .map(|(name, _, _)| {
            let name_str = name.unraw().to_string().to_case(Case::Kebab);
            syn::LitStr::new(&name_str, proc_macro2::Span::call_site())
        })
        .collect();

    // Generate field names for Query Into<Parameters>
    let all_field_names: Vec<_> = all_fields.iter().map(|(name, _, _, _, _)| name).collect();
    let all_field_name_lits: Vec<_> = all_fields
        .iter()
        .map(|(name, _, _, _, _)| {
            let name_str = name.unraw().to_string().to_case(Case::Kebab);
            syn::LitStr::new(&name_str, proc_macro2::Span::call_site())
        })
        .collect();

    // Generate Formula::write() statements: only output fields are written
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
                #name: #ty::try_from(source.lookup(&dialog_query::Term::from(&self.#name))?.content()?)?
            }
        })
        .collect();

    let input_struct_doc = syn::LitStr::new(
        &format!(
            "Input structure for the [`{struct_name}`] formula. Holds only the required (non-output) fields that must be provided to compute the formula.",
        ),
        proc_macro2::Span::call_site(),
    );
    let query_struct_doc = syn::LitStr::new(
        &format!(
            "Query pattern for the [`{struct_name}`] formula. Holds every field (input and output) as a [`dialog_query::Term`] for pattern matching.",
        ),
        proc_macro2::Span::call_site(),
    );

    // Impls that convert through `FormulaQuery` only exist for the
    // instantiation(s) registered in `define_formulas!` (the
    // canonical dynamic instantiation for generic formulas), so they
    // are bounded on that conversion existing.
    let mut fq_where: syn::WhereClause = generics
        .where_clause
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(where));
    fq_where.predicates.push(syn::parse_quote!(
        dialog_query::FormulaQuery: ::std::convert::From<#query_name #ty_generics>
    ));

    let expanded = quote! {
            /// Input structure for #struct_name formula
            ///
            /// Contains only the required (non-output) fields that must be provided
            /// to compute the formula.
            #[doc = #input_struct_doc]
            #[derive(Debug, Clone)]
            pub struct #input_name #generics #where_clause {
                #(#input_struct_fields),*
            }

            /// Query pattern for #struct_name formula.
            ///
            /// Contains all fields (both input and output) as Term<T> for pattern matching.
            #[doc = #query_struct_doc]
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct #query_name #generics #where_clause {
                #(#query_struct_fields),*
            }

            /// Static storage for formula cells
            static #cells_name: ::std::sync::OnceLock<dialog_query::Cells> = ::std::sync::OnceLock::new();

            impl #impl_generics dialog_query::Predicate for #struct_name #ty_generics #fq_where {
                type Conclusion = #struct_name #ty_generics;
                type Application = #query_name #ty_generics;
                type Descriptor = dialog_query::Entity;
            }

            impl #impl_generics dialog_query::Application for #query_name #ty_generics #fq_where
    {
                type Conclusion = #struct_name #ty_generics;

                fn evaluate<'__a, __Env, __M: dialog_query::Selection + '__a>(
                    self,
                    selection: __M,
                    _env: &'__a __Env,
                ) -> impl dialog_query::Selection + '__a
                where
                    __Env: dialog_query::Provider<dialog_query::Select<'__a>>
                        + dialog_query::Provider<dialog_query::source::SelectRules>
                        + dialog_query::ConditionalSync,
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

            impl #impl_generics ::std::convert::From<#query_name #ty_generics> for dialog_query::Parameters #where_clause {
                fn from(terms: #query_name #ty_generics) -> Self {
                    let mut parameters = Self::new();
                    #(parameters.insert(#all_field_name_lits.into(), dialog_query::Term::<dialog_query::types::Any>::from(terms.#all_field_names));)*
                    parameters
                }
            }

            impl #impl_generics From<#query_name #ty_generics> for dialog_query::Premise #fq_where
    {
                fn from(source: #query_name #ty_generics) -> Self {
                    let formula: dialog_query::FormulaQuery = source.into();
                    dialog_query::Premise::Assert(dialog_query::Proposition::from(formula))
                }
            }

            impl #impl_generics From<#query_name #ty_generics> for dialog_query::Proposition #fq_where
    {
                fn from(source: #query_name #ty_generics) -> Self {
                    let formula: dialog_query::FormulaQuery = source.into();
                    dialog_query::Proposition::from(formula)
                }
            }

            impl #impl_generics ::std::ops::Not for #query_name #ty_generics #fq_where
    {
                type Output = dialog_query::Premise;

                fn not(self) -> Self::Output {
                    let proposition: dialog_query::Proposition = self.into();
                    dialog_query::Premise::Unless(dialog_query::Negation(proposition))
                }
            }

            impl #impl_generics ::std::convert::TryFrom<&mut dialog_query::Bindings> for #input_name #ty_generics #where_clause {
                type Error = dialog_query::EvaluationError;

                fn try_from(bindings: &mut dialog_query::Bindings) -> ::std::result::Result<Self, Self::Error> {
                    Ok(#input_name {
                        #(#input_field_names: bindings.resolve(#input_field_name_lits)?.try_into()?),*
                    })
                }
            }

            impl #impl_generics dialog_query::Formula for #struct_name #ty_generics #fq_where {
                type Input = #input_name #ty_generics;

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

                fn apply(terms: dialog_query::Parameters) -> ::std::result::Result<#query_name #ty_generics, dialog_query::error::TypeError> {
                    let cells = <#struct_name #ty_generics as dialog_query::Formula>::cells();
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
