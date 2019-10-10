use crate::message::MessageType;
use crate::message2::Message;
use crate::notification::Notification;
use crate::result::Result;
use bytes::Buf;
use failure::format_err;
use std::io::Cursor;

pub struct MessageDecoder<'a> {
    buffer: Cursor<&'a [u8]>,
}

impl<'a> MessageDecoder<'a> {
    pub(crate) fn decode(buffer: &[u8]) -> Result<Message> {
        MessageDecoder {
            buffer: Cursor::new(buffer),
        }
        .decode_message()
    }

    fn decode_message(&mut self) -> Result<Message> {
        Ok(Message {
            message_type: self.decode_message_type()?,
            sequence_number: self.decode_sequence_number()?,
            notifications: self.decode_notifications()?,
        })
    }

    fn decode_message_type(&mut self) -> Result<MessageType> {
        let message_type = self.buffer.get_i32_be();
        match message_type {
            x if x == MessageType::Ping as i32 => Ok(MessageType::Ping),
            x if x == MessageType::PingAck as i32 => Ok(MessageType::PingAck),
            x if x == MessageType::PingIndirect as i32 => Ok(MessageType::PingIndirect),
            x => Err(format_err!("Unsupported message type: {}", x)),
        }
    }

    fn decode_sequence_number(&mut self) -> Result<u64> {
        if self.buffer.remaining() < std::mem::size_of::<u64>() {
            return Err(format_err!("Not enough bytes to discover message sequence number"));
        }
        Ok(self.buffer.get_u64_be())
    }

    fn decode_notifications(&mut self) -> Result<Vec<Notification>> {
        if !self.buffer.has_remaining() {
            return Ok(Vec::new());
        }

        let count = self.buffer.get_u8();
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            result.push(self.decode_notification()?);
        }
        Ok(result)
    }

    fn decode_notification(&mut self) -> Result<Notification> {
        if !self.buffer.has_remaining() {
            return Err(format_err!("Unable to decode notification header"));
        }
        let header = self.buffer.get_u8();
        let notification_type = header & 0x0f;
        let address_type = header >> 7;
        Ok(Notification {})
    }

    fn decode_notification_type(&mut self, notification_type: u8) -> NotificationType {}
}

fn decode_message(buffer: &[u8]) -> Result<Message> {
    // 1. check protocol version in buffer
    // 2. create proper decoder
    // 3. decode and return message
    MessageDecoder::decode(buffer)
}
