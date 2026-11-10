//! The `#[provider]` attribute: a `Provider` impl whose `execute` is
//! written as an `async fn`.
//!
//! `Provider::execute` answers with a `Resume`, which is either the output
//! at once or a future that produces it. Most providers always have to
//! wait, and write `execute` as an `async fn`; this attribute turns that
//! into an `execute` that answers with the body as its future. An
//! `execute` that is not `async` is left as written, so an impl that has
//! the answer at hand on some paths returns `Resume::now` there and
//! `Resume::later` on the rest, by hand.

use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemImpl, ReturnType, parse_macro_input, parse_quote};

/// Implementation of the `dialog_capability::provider` attribute.
pub fn generate(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new_spanned(
            proc_macro2::TokenStream::from(attr),
            "#[provider] does not accept any arguments",
        )
        .to_compile_error()
        .into();
    }
    let mut input = parse_macro_input!(item as ItemImpl);
    for item in &mut input.items {
        if let ImplItem::Fn(method) = item
            && let Some(asyncness) = method.sig.asyncness.take()
        {
            if method.sig.ident != "execute" {
                return syn::Error::new_spanned(
                    asyncness,
                    "#[provider] rewrites only `async fn execute`",
                )
                .to_compile_error()
                .into();
            }
            match method.sig.inputs.first() {
                Some(FnArg::Receiver(receiver))
                    if receiver.reference.is_some() && receiver.mutability.is_none() => {}
                _ => {
                    return syn::Error::new_spanned(
                        &method.sig,
                        "a provider's `execute` takes `&self`",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            let output = match &method.sig.output {
                ReturnType::Default => quote! { () },
                ReturnType::Type(_, ty) => quote! { #ty },
            };
            method.sig.output = parse_quote! { -> ::dialog_capability::Resume<'_, #output> };
            let body = &method.block;
            // The body's tail and its `return`s are typed against the
            // declared output, so a `Box<Concrete>` still coerces to the
            // `Box<dyn Trait>` the signature names, as it did in an
            // `async fn`.
            method.block = parse_quote! {{
                ::dialog_capability::Resume::later(async move {
                    let __output: #output = #body;
                    #[allow(unreachable_code)]
                    __output
                })
            }};
            method
                .attrs
                .push(parse_quote! { #[allow(clippy::absolute_paths)] });
        }
    }
    quote! { #input }.into()
}
