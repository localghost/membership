extern crate proc_macro;

use proc_macro::TokenStream;
use syn::parse::{Parse, ParseBuffer, ParseStream};
use syn::Error;

struct MyEncoderInput {
    seqno: syn::LitInt,
}

impl Parse for MyEncoderInput {
    fn parse(input: &ParseStream) -> Result<Self, Error> {
        let ident = syn::Ident::parse(input)?;
        Ok(MyEncoderInput { seqno: ident. })
    }
}

struct Encoder {}

impl Encoder {
    fn encode(seqno: i32) -> i32 {
        seqno * 4
    }
}

#[proc_macro]
pub fn encode(input: TokenStream) -> TokenStream {}
