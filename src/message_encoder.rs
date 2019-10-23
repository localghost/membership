use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, Bytes, BytesMut};
use failure::format_err;
use std::net::SocketAddr;

struct PingRequestMessage {
    buffer: Bytes,
}

struct PingMessage {}
struct AckMessage {}

struct OutgoingMessage {
    buffer: Bytes,
}

fn encode_message_ping_request(sequence_number: u64, target: SocketAddr) -> OutgoingMessage {
    let mut buffer = BytesMut::new();
    buffer.put_u8(0u8);
    buffer.put_u64_be(sequence_number);
    match target {
        SocketAddr::V4(address) => {
            buffer.put_slice(&address.ip().octets());
            buffer.put_u16_be(address.port());
        }
        SocketAddr::V6(address) => {
            unimplemented!();
        }
    };
    OutgoingMessage {
        buffer: buffer.freeze(),
    }
}

pub(crate) fn encode_message(max_size: usize) -> OutgoingMessageWithType {
    OutgoingMessageWithType {
        encoder: MessageEncoder {
            message: OutgoingMessage {
                buffer: BytesMut::with_capacity(max_size),
            },
        },
    }
}

struct OutgoingMessageWithType {
    encoder: MessageEncoder,
}

impl OutgoingMessageWithType {
    fn message_type(mut self, message_type: MessageType) -> Result<OutgoingMessageWithSequenceNumber> {
        self.encoder.message_type(message_type)?;
        Ok(OutgoingMessageWithSequenceNumber { encoder: self.encoder })
    }
}

struct OutgoingMessageWithSequenceNumber {
    encoder: MessageEncoder,
}

impl OutgoingMessageWithSequenceNumber {
    fn sequence_number(mut self, sequence_number: u64) -> Result<OutgoingMessageWithNotifications> {
        self.encoder.sequence_number(sequence_number)?;
        Ok(OutgoingMessageWithNotifications { encoder: self.encoder })
    }
}

struct OutgoingMessageWithNotifications {
    encoder: MessageEncoder,
}

impl OutgoingMessageWithNotifications {
    fn notifications(mut self, notifications: &[Notification]) -> Result<OutgoingMessageWithBroadcast> {
        self.encoder.message.num_notifications = self.encoder.notifications(notifications)?;
        Ok(OutgoingMessageWithBroadcast { encoder: self.encoder })
    }
}

struct OutgoingMessageWithBroadcast {
    encoder: MessageEncoder,
}

impl OutgoingMessageWithBroadcast {
    fn broadcast(&mut self, members: &[Member]) -> Result<()> {
        self.encoder.message.num_broadcast = self.encoder.broadcast(members)?;
        Ok(())
    }

    fn build(mut self) -> Bytes {
        self.encoder.message.buffer.freeze()
    }
}

struct MessageEncoder {
    message: OutgoingMessage,
}

impl MessageEncoder {
    fn message_type(&mut self, message_type: MessageType) -> Result<()> {
        if self.message.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.message.buffer.put_i32_be(message_type as i32);
        Ok(())
    }

    fn sequence_number(&mut self, sequence_number: u64) -> Result<()> {
        if self.message.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.message.buffer.put_u64_be(sequence_number);
        Ok(())
    }

    fn encode_notification(&mut self, notification: &Notification) -> Result<()> {
        match notification {
            Notification::Alive { member } => {
                self.message.buffer.put_u8(0);
                self.encode_member(member)?;
            }
            Notification::Suspect { member } => {
                self.message.buffer.put_u8(1);
                self.encode_member(member);
            }
            Notification::Confirm { member } => {
                self.message.buffer.put_u8(2);
                self.encode_member(member)?;
            }
            Notification::Check { member } => {
                self.message.buffer.put_u8(3);
                self.encode_member(member)?;
            }
        }
        Ok(())
    }

    fn notification_size(&self, notification: &Notification) -> usize {
        match notification {
            Notification::Alive { member } | Notification::Confirm { member } | Notification::Suspect { member } => {
                1 + self.member_size(member)
            }
        }
    }

    fn notifications(&mut self, notifications: &[Notification]) -> Result<usize> {
        if !self.message.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode notifications"));
        }
        let count_position = self.message.buffer.len();
        self.message.buffer.resize(self.message.buffer.len() + 1, 0u8);
        let mut count = 0;
        for notification in notifications {
            // -1 so that there is space for broadcast header
            if self.notification_size(notification) > (self.message.buffer.remaining_mut() - 1) {
                break;
            }
            self.encode_notification(notification)?;
            count += 1;
        }
        self.message.buffer[count_position] = count;
        Ok(count as usize)
    }

    fn broadcast(&mut self, members: &[Member]) -> Result<usize> {
        if !self.message.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode broadcast"));
        }
        let count_position = self.message.buffer.len();
        self.message.buffer.resize(self.message.buffer.len() + 1, 0u8);
        let mut count = 0;
        for member in members {
            if self.member_size(member) > self.message.buffer.remaining_mut() {
                break;
            }
            self.encode_member(member)?;
            count += 1;
        }
        self.message.buffer[count_position] = count;
        Ok(count as usize)
    }

    fn build(self) -> Bytes {
        self.message.buffer.freeze()
    }

    fn member_size(&self, member: &Member) -> usize {
        if member.address.is_ipv4() {
            // IP address + UDP port + incarnation number
            4 + 2 + 8
        } else {
            unimplemented!()
        }
    }

    fn encode_member(&mut self, member: &Member) -> Result<()> {
        match member.address {
            SocketAddr::V4(address) => {
                self.message.buffer.put_slice(&address.ip().octets());
                self.message.buffer.put_u16_be(address.port());
            }
            SocketAddr::V6(address) => {
                unimplemented!();
            }
        }
        Ok(())
    }
}
