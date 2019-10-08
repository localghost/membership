use crate::message2::Message;
use crate::result::Result;

mod v1;

fn decode_message(buffer: &[u8]) -> Result<Message> {
    // 1. check protocol version in buffer
    // 2. create proper decoder
    // 3. decode and return message
    v1::MessageDecoder::decode(buffer)
}
