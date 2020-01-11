extern crate proc_macro;

use proc_macro::TokenStream;

#[proc_macro]
pub fn encode(input: TokenStream) -> TokenStream {
    TokenStream::new()
}
