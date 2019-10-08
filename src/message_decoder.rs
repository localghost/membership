use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::BytesMut;

struct Message {
    type_: MessageType,
    sequence_number: u64,
    notifications: Vec<Notification>,
}

impl Message {
    fn message_type(&self) -> MessageType {
        self.type_
    }

    fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    fn notifications(&self) -> Vec<Notification> {
        self.notifications.clone()
    }
}

struct MessageDecoderV1 {
    buffer: BytesMut,
}

impl MessageDecoderV1 {
    fn decode(buffer: &[u8]) -> Result<Message> {
        MessageDecoderV1 {
            buffer: BytesMut::from(buffer),
        }
        .decode_message()
    }

    fn decode_message(&self) -> Result<Message> {
        Ok(Message {
            type_: self.decode_message_type()?,
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

fn decode_message(buffer: &[u8]) -> Result<Message> {
    // 1. check protocol version in buffer
    // 2. create proper decoder
    // 3. decode and return message
    MessageDecoderV1::decode(buffer)
}
