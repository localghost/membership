use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, Bytes, BytesMut, IntoBuf};
use failure::format_err;
use std::net::SocketAddr;

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
        if !self.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode notifications"));
        }
        let count_position = self.buffer.len();
        self.buffer.resize(self.buffer.len() + 1, 0u8);
        let mut count = 0;
        for notification in notifications {
            // -1 so that there is space for broadcast header
            if self.notification_size(notification) > (self.buffer.remaining_mut() - 1) {
                break;
            }
            self.encode_notification(notification)?;
            count += 1;
        }
        self.buffer[count_position] = count;
        Ok(count as usize)
    }

    fn broadcast(&mut self, members: &[Member]) -> Result<usize> {
        if !self.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode notifications"));
        }
        let count_position = self.buffer.len();
        self.buffer.resize(self.buffer.len() + 1, 0u8);
        let mut count = 0;
        for member in members {
            if self.member_size(member) > self.buffer.remaining_mut() {
                break;
            }
            self.encode_member(member)?;
            count += 1;
        }
        self.buffer[count_position] = count;
        Ok(count as usize)
    }

    fn build(self) -> Bytes {
        self.buffer.freeze()
    }

    fn notification_size(&self, notification: &Notification) -> usize {
        match notification {
            Notification::Alive { member } | Notification::Confirm { member } | Notification::Suspect { member } => {
                1 + self.member_size(member)
            }
        }
    }

    fn member_size(&self, member: &Member) -> usize {
        if member.address.is_ipv4() {
            // IP address + UDP port + incarnation number
            4 + 2 + 8
        } else {
            unimplemented!()
        }
    }

    fn encode_notification(&mut self, notification: &Notification) -> Result<()> {
        match notification {
            Notification::Alive { member } => {
                self.buffer.put_u8(0);
                self.encode_member(member)?;
            }
            Notification::Suspect { member } => {
                self.buffer.put_u8(1);
                self.encode_member(member);
            }
            Notification::Confirm { member } => {
                self.buffer.put_u8(2);
                self.encode_member(member)?;
            }
        }
        Ok(())
    }

    fn encode_member(&mut self, member: &Member) -> Result<()> {
        match member.address {
            SocketAddr::V4(address) => {
                self.buffer.put_slice(&address.ip().octets());
                self.buffer.put_u16_be(address.port());
            }
            SocketAddr::V6(address) => {
                unimplemented!();
            }
        }
        Ok(())
    }
}
