use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, BytesMut};
use failure::format_err;

struct RawMessage {
    buffer: BytesMut,
}

struct OutgoingMessage {
    buffer: BytesMut,
}

impl OutgoingMessage {
    fn new(max_size: usize) -> Self {
        OutgoingMessage {
            buffer: BytesMut::with_capacity(max_size),
        }
    }

    fn message_type(&mut self, message_type: MessageType) -> Result<()> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.buffer.put_i32_be(message_type as i32);
        Ok(())
    }

    fn sequence_number(&mut self, sequence_number: u64) -> Result<()> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.buffer.put_u64_be(sequence_number);
        Ok(())
    }

    fn notifications(&mut self, notifications: &[Notification]) -> Result<usize> {
        let mut cursor = std::io::Cursor::new(&self.buffer);
        let count_position = cursor.position();
        for notification in notifications {
            self.buffer.
            let position = cursor.position();
            //            self.encode_notification();
            self.buffer.put_slice()
        }
    }

    fn broadcast(members: &[Member]) -> usize {}

    fn build(self) -> RawMessage {
        RawMessage { buffer: self.buffer }
    }

    fn encode_message_type(&mut self, message_type: MessageType) {}
}

//struct MessageEncoder {
//    max_size: usize,
//}
//
//impl MessageEncoder {
//    fn new(max_size: usize) -> Self {
//        MessageEncoder { max_size }
//    }
//
//    fn message(message_type: MessageType, sequence_number: u64) -> OutgoingMessage {
//        let buffer = BytesMut::with_capacity(max_size);
//    }
//}
