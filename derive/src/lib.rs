//! `#[derive(Serve)]`: serve a fold3 schema's fields over HTTP.
//!
//! Sits beside `#[derive(Schema)]` on the same struct and, like it,
//! never inspects field types: each field becomes an `Entry` built from
//! its type's `Resource` impl, so a field whose type isn't servable is a
//! type error naming that type.
//!
//! - an unannotated field is a source: readable and writable;
//! - a `#[view]` field is read-only;
//! - `#[serve(read_only)]` serves a source's reads only: its writes stay
//!   with the application's own routes;
//! - `#[serve(skip)]` leaves a field unserved;
//! - `#[schema]` (nested schemas) must be skipped for now.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Ts};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Serve, attributes(serve))]
pub fn derive_serve(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<Ts> {
    let name = &input.ident;
    let Data::Struct(s) = &input.data else {
        return Err(syn::Error::new(
            Span::call_site(),
            "Serve derives only for structs",
        ));
    };
    let Fields::Named(fields) = &s.fields else {
        return Err(syn::Error::new(
            Span::call_site(),
            "Serve needs named fields",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Serve structs cannot be generic",
        ));
    }

    let p = quote!(::fold_http::__private);
    let mut entries = Vec::new();
    for f in &fields.named {
        let ident = f.ident.as_ref().expect("named field");
        let ty = &f.ty;
        let serve = options(f)?;
        if serve.skip {
            continue;
        }
        let is = |attr: &str| f.attrs.iter().any(|a| a.path().is_ident(attr));
        if serve.read_only && is("view") {
            return Err(syn::Error::new_spanned(
                ident,
                "views are always read-only; drop #[serve(read_only)]",
            ));
        }
        if is("schema") {
            return Err(syn::Error::new_spanned(
                ident,
                "fold-http doesn't serve nested schemas yet; mark the field #[serve(skip)]",
            ));
        }
        let field_name = ident.to_string();
        let field_name = field_name.strip_prefix("r#").unwrap_or(&field_name);
        if field_name.starts_with('_') {
            return Err(syn::Error::new_spanned(
                ident,
                "`_`-prefixed names are reserved for fold-http's own routes; rename the field or #[serve(skip)] it",
            ));
        }
        let doc = doc_comment(f);
        let describe = if doc.is_empty() {
            quote!()
        } else {
            quote!(.description(#doc))
        };
        // Each field's key and value encoders: `Id` as its raw number,
        // anything else through serde.
        let via = quote_spanned! {ty.span()=>
            (
                (&&#p::Probe::<<#ty as ::fold_http::Shape>::Key>::NEW).via(),
                (&&#p::Probe::<<#ty as ::fold_http::Shape>::Value>::NEW).via(),
            )
        };
        entries.push(if is("view") || serve.read_only {
            quote_spanned! {ty.span()=>
                ::fold_http::Entry::<Self>::view::<#ty, _>(
                    #field_name,
                    #via,
                    |r, k| k(&r.#ident),
                ) #describe
            }
        } else {
            quote_spanned! {ty.span()=>
                ::fold_http::Entry::<Self>::source::<#ty, _>(
                    #field_name,
                    #via,
                    |r, k| k(&r.#ident),
                    |w, k| k(&w.#ident),
                    (&&#p::Probe::<#ty>::NEW).mint(),
                ) #describe
            }
        });
    }

    Ok(quote! {
        impl ::fold_http::Serve for #name {
            fn entries() -> ::std::vec::Vec<::fold_http::Entry<Self>> {
                #[allow(unused_imports)]
                use #p::{MintById as _, MintNone as _, ViaIdProbe as _, ViaSerdeProbe as _};
                ::std::vec![#(#entries),*]
            }
        }
    })
}

/// A field's `#[serve(...)]` options.
#[derive(Default)]
struct Options {
    skip: bool,
    read_only: bool,
}

fn options(f: &syn::Field) -> syn::Result<Options> {
    let mut o = Options::default();
    for attr in f.attrs.iter().filter(|a| a.path().is_ident("serve")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                o.skip = true;
                Ok(())
            } else if meta.path.is_ident("read_only") {
                o.read_only = true;
                Ok(())
            } else {
                Err(meta.error("expected `skip` or `read_only`"))
            }
        })?;
    }
    if o.skip && o.read_only {
        return Err(syn::Error::new_spanned(
            f.ident.as_ref(),
            "a skipped field isn't served at all; use `skip` or `read_only`, not both",
        ));
    }
    Ok(o)
}

/// A field's `///` comment, one line per `#[doc]`, common indent removed.
fn doc_comment(f: &syn::Field) -> String {
    let lines: Vec<String> = f
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .filter_map(|a| match &a.meta {
            syn::Meta::NameValue(nv) => match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) => Some(s.value()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    let indent = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    lines
        .iter()
        .map(|l| l.get(indent..).unwrap_or("").trim_end())
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}
