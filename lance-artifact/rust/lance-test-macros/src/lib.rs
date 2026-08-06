// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;

use quote::quote;
use syn::{FnArg, ItemFn, ReturnType, Token, parse_macro_input, punctuated::Punctuated};

// The tracing initialization
//
// Note that there are two guards.  The first is for the chrome layer and the
// second is for the tracing subscriber.  The tuple order is important as the
// chrome layer guard must be dropped before the subscriber guard.
fn expand_tracing_init() -> Tokens {
    quote! {
      {
        let trace_level = std::env::var("LANCE_TRACING");
        if let Ok(trace_level) = trace_level {

          let level_filter = match trace_level.as_str() {
            "debug" => ::tracing_subscriber::filter::LevelFilter::DEBUG,
            "info" => ::tracing_subscriber::filter::LevelFilter::INFO,
            "warn" => ::tracing_subscriber::filter::LevelFilter::WARN,
            "error" => ::tracing_subscriber::filter::LevelFilter::ERROR,
            "trace" => ::tracing_subscriber::filter::LevelFilter::TRACE,
            _ => panic!("Unexpected trace level {}", trace_level),
          };

          let (chrome_layer, _guard) = tracing_chrome::ChromeLayerBuilder::new().trace_style(tracing_chrome::TraceStyle::Async).include_args(true).build();
          let subscriber = ::tracing_subscriber::registry::Registry::default();
          let chrome_layer = ::tracing_subscriber::Layer::with_filter(chrome_layer, level_filter);
          let subscriber = tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt::with(
            subscriber, chrome_layer);
          let sub_guard = ::tracing::subscriber::set_default(subscriber);
          Some((_guard, sub_guard))
        } else {
          None
        }
      }
    }
}

fn extract_args(inputs: &Punctuated<FnArg, Token![,]>) -> Punctuated<Tokens, Token![,]> {
    inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Receiver(receiver) => {
                let slf = receiver.self_token;
                quote! { #slf }
            }
            FnArg::Typed(typed) => {
                let pat = &typed.pat;
                quote! { #pat }
            }
        })
        .collect()
}

// This function parses the wrapped object into a function (tests are functions) and
// then creates a new wrapped function
fn expand_wrapper(wrapped_attr: Tokens, wrappee: ItemFn) -> Tokens {
    let attrs = &wrappee.attrs;
    let async_ = &wrappee.sig.asyncness;
    let await_ = if async_.is_some() {
        quote! {.await}
    } else {
        quote! {}
    };
    let body = &wrappee.block;
    let test_name = &wrappee.sig.ident;
    let inputs = &wrappee.sig.inputs;
    let args = extract_args(inputs);

    // Note that Rust does not allow us to have a test function with
    // #[should_panic] that has a non-unit return value.
    let ret = match &wrappee.sig.output {
        ReturnType::Default => quote! {},
        ReturnType::Type(_, type_) => quote! {-> #type_},
    };

    let tracing_init = expand_tracing_init();

    // Creates a test-scoped init function and then calls the underlying test
    let result = quote! {
      #[#wrapped_attr]
      #(#attrs)*
      #async_ fn #test_name(#inputs) #ret {
        #async_ fn test_impl(#inputs) #ret {
          #body
        }

        mod init {
          pub fn init() -> Option<(tracing_chrome::FlushGuard, tracing::subscriber::DefaultGuard)> {
            #tracing_init
          }
        }

        let _guard = init::init();
        test_impl(#args)#await_
      }
    };
    result
}

// Note: this is a fork of https://crates.io/crates/test-log
//
// The original crate could only configure logging tracing to stdout and this
// is a good entrypoint for any other lance-specific test behaviors we want to
// add in the future.
/// This attribute wraps any existing test attribute (e.g. tokio::test or test)
/// to configure tracing
///
/// Example:
///
/// ```rust,ignore
/// #[lance_test_macros::test(tokio::test)]
/// async fn test_something() {
///  ...
/// }
/// ```
///
/// By default this wrapper will do nothing.  To then get tracing output, set the
/// LANCE_TRACING environment variable to your desired level (e.g. "debug").
///
/// Example:
///
/// ```bash
/// LANCE_TRACING=debug cargo test dataset::tests::test_create_dataset
/// ```
///
/// A .json file will be generated in the current directory that can be loaded into
/// chrome://tracing or the perfetto UI.
///
/// Note: if multiple tests are wrapped and you enable the environment variable then
/// you will get a separate .json file for each test that is run.  So generally you
/// only want to set the environment variable when running a single test at a time.
#[proc_macro_attribute]
pub fn test(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);

    expand_wrapper(args.into(), input).into()
}
