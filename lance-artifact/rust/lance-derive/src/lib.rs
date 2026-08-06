// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro for the `DeepSizeOf` trait.
///
/// Generates an implementation that sums the `deep_size_of_children` of all
/// fields (for structs) or the active variant's fields (for enums).
#[proc_macro_derive(DeepSizeOf)]
pub fn derive_deep_size_of(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;

    // Add DeepSizeOf bounds to all type parameters
    let mut bounded_generics = generics.clone();
    for param in &mut bounded_generics.params {
        if let syn::GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(syn::parse_quote!(lance_core::deepsize::DeepSizeOf));
        }
    }
    let (impl_generics, _, where_clause) = bounded_generics.split_for_impl();
    let (_, ty_generics, _) = generics.split_for_impl();

    let body = match &input.data {
        Data::Struct(data) => generate_struct_body(&data.fields),
        Data::Enum(data) => {
            let arms: Vec<_> = data
                .variants
                .iter()
                .map(|variant| {
                    let variant_ident = &variant.ident;
                    match &variant.fields {
                        Fields::Unit => {
                            quote! { Self::#variant_ident => 0 }
                        }
                        Fields::Unnamed(fields) => {
                            let bindings: Vec<_> = (0..fields.unnamed.len())
                                .map(|i| {
                                    syn::Ident::new(
                                        &format!("__field_{}", i),
                                        proc_macro2::Span::call_site(),
                                    )
                                })
                                .collect();
                            let sum = bindings.iter().map(|b| {
                                quote! { lance_core::deepsize::DeepSizeOf::deep_size_of_children(#b, __context) }
                            });
                            quote! {
                                Self::#variant_ident(#(#bindings),*) => {
                                    0 #(+ #sum)*
                                }
                            }
                        }
                        Fields::Named(fields) => {
                            let field_names: Vec<_> =
                                fields.named.iter().map(|f| &f.ident).collect();
                            let sum = field_names.iter().map(|f| {
                                quote! { lance_core::deepsize::DeepSizeOf::deep_size_of_children(#f, __context) }
                            });
                            quote! {
                                Self::#variant_ident { #(#field_names),* } => {
                                    0 #(+ #sum)*
                                }
                            }
                        }
                    }
                })
                .collect();
            quote! {
                match self {
                    #(#arms),*
                }
            }
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(&input, "DeepSizeOf cannot be derived for unions")
                .to_compile_error()
                .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics lance_core::deepsize::DeepSizeOf for #name #ty_generics #where_clause {
            fn deep_size_of_children(&self, __context: &mut lance_core::deepsize::Context) -> usize {
                #body
            }
        }
    };

    TokenStream::from(expanded)
}

fn generate_struct_body(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields) => {
            let field_sizes = fields.named.iter().map(|f| {
                let name = &f.ident;
                quote! { lance_core::deepsize::DeepSizeOf::deep_size_of_children(&self.#name, __context) }
            });
            quote! { 0 #(+ #field_sizes)* }
        }
        Fields::Unnamed(fields) => {
            let field_sizes = (0..fields.unnamed.len()).map(|i| {
                let index = syn::Index::from(i);
                quote! { lance_core::deepsize::DeepSizeOf::deep_size_of_children(&self.#index, __context) }
            });
            quote! { 0 #(+ #field_sizes)* }
        }
        Fields::Unit => {
            quote! { 0 }
        }
    }
}
