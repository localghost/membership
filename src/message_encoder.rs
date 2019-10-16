use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, Bytes, BytesMut, IntoBuf};
use failure::format_err;

struct OutgoingMessage {}

impl OutgoingMessage {
    fn new(max_size: usize) -> OutgoingMessageWithType {
        OutgoingMessageWithType {
            buffer: BytesMut::with_capacity(max_size),
        }
    }
}

struct OutgoingMessageWithType {
    buffer: BytesMut,
}

impl OutgoingMessageWithType {
    fn message_type(mut self, message_type: MessageType) -> Result<OutgoingMessageWithSequenceNumber> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.buffer.put_i32_be(message_type as i32);
        Ok(OutgoingMessageWithSequenceNumber { buffer: self.buffer })
    }
}

struct OutgoingMessageWithSequenceNumber {
    buffer: BytesMut,
}

impl OutgoingMessageWithSequenceNumber {
    fn sequence_number(mut self, sequence_number: u64) -> Result<OutgoingMessageWithBuild> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.buffer.put_u64_be(sequence_number);
        Ok(OutgoingMessageWithBuild { buffer: self.buffer })
    }
}

struct OutgoingMessageWithBuild {
    buffer: BytesMut,
}

impl OutgoingMessageWithBuild {
    fn notifications(&mut self, notifications: &[Notification]) -> Result<usize> {
        let mut cursor = std::io::Cursor::new(&self.buffer);
        let count_position = cursor.position();
        for notification in notifications {
            if self.notification_size(notification) > self.buffer.remaining_mut() {
                break;
            }
            self.encode_notification(notification)?;
            //            self.buffer.
            //            let position = cursor.position();
            //            //            self.encode_notification();
            //            self.buffer.put_slice()
        }
        Ok(0)
    }

    fn broadcast(members: &[Member]) -> usize {}

    fn build(self) -> Bytes {
        self.buffer.freeze()
    }

    fn notification_size(&self, notification: &Notification) -> usize {
        match notification {
            Notification::Alive { member } | Notification::Confirm { member } | Notification::Suspect { member } => {
                if member.address.is_ipv4() {
                    // notification header + IP address + UDP port + incarnation number
                    1 + 4 + 2 + 8
                } else {
                    unimplemented!()
                }
            }
        }
    }

    fn encode_notification(&mut self, notification: &Notification) -> Result<()> {
        Ok(())
    }

    fn encode_member(&mut self) -> Result<()> {
        Ok(())
    }
}
