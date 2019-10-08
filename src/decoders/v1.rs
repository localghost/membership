use crate::message::MessageType;
use crate::message2::Message;
use crate::notification::Notification;
use crate::result::Result;
use bytes::BytesMut;

pub struct MessageDecoder {
    buffer: BytesMut,
}

impl MessageDecoder {
    pub(crate) fn decode(buffer: &[u8]) -> Result<Message> {
        MessageDecoder {
            buffer: BytesMut::from(buffer),
        }
        .decode_message()
    }

    fn decode_message(&self) -> Result<Message> {
        Ok(Message {
            message_type: self.decode_message_type()?,
            sequence_number: self.decode_sequence_number()?,
            notifications: self.decode_notifications()?,
        })
    }

    fn decode_message_type(&self) -> Result<MessageType> {
        Ok(MessageType::Ping)
    }

    fn decode_sequence_number(&self) -> Result<u64> {
        Ok(0)
    }

    fn decode_notifications(&self) -> Result<Vec<Notification>> {
        Ok(Vec::new())
    }
}
