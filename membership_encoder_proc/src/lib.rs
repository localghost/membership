extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro_hack::proc_macro_hack;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::parse_macro_input;
use syn::Error;

struct MyEncoderInput {
    seqno: syn::LitInt,
}

impl Parse for MyEncoderInput {
    fn parse(input: ParseStream) -> Result<Self, Error> {
        let ident = syn::Ident::parse(input)?;
        let seqno;
        syn::parenthesized!(seqno in input);
        Ok(MyEncoderInput { seqno: seqno.parse()? })
    }
}

impl Into<proc_macro2::TokenStream> for MyEncoderInput {
    fn into(self) -> proc_macro2::TokenStream {
        let seqno: i32 = self.seqno.base10_parse().unwrap();
        return quote! {
            Encoder::encode(#seqno);
        };
    }
}

struct Encoder {}

impl Encoder {
    fn encode(seqno: i32) -> i32 {
        seqno * 4
    }
}

#[proc_macro_hack]
pub fn encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as MyEncoderInput);
    let output: proc_macro2::TokenStream = input.into();
    output.into()
}
