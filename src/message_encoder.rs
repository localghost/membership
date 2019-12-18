use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, Bytes, BytesMut};
use failure::format_err;
use std::net::SocketAddr;

#[derive(Debug)]
struct PingRequestMessage {
    buffer: Bytes,
}

struct PingMessage {}
struct AckMessage {}

#[derive(Debug)]
struct OutgoingMessage {
    buffer: BytesMut,
    num_notifications: usize,
    num_broadcast: usize,
}

impl OutgoingMessage {
    pub(crate) fn num_notifications(&self) -> usize {
        self.num_notifications
    }

    pub(crate) fn num_broadcast(&self) -> usize {
        self.num_broadcast
    }
}

#[derive(Debug)]
pub(crate) enum OutgoingMessageEnum {
    DisseminationMessage(OutgoingMessage),
    PingRequestMessage(PingRequestMessage),
}

impl OutgoingMessageEnum {
    pub(crate) fn buffer(self) -> Bytes {
        match self {
            OutgoingMessageEnum::DisseminationMessage(message) => message.buffer.freeze(),
            OutgoingMessageEnum::PingRequestMessage(message) => message.buffer,
        }
    }
}

fn encode_message_ping_request(sequence_number: u64, target: SocketAddr) -> PingRequestMessage {
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
    PingRequestMessage {
        buffer: buffer.freeze(),
    }
}

//pub(crate) fn encode_message(max_size: usize) -> OutgoingMessageWithType {
//    OutgoingMessageWithType {
//        encoder: MessageEncoder {
//            message: OutgoingMessage {
//                buffer: BytesMut::with_capacity(max_size),
//                num_notifications: 0,
//                num_broadcast: 0,
//            },
//        },
//    }
//}

//struct OutgoingMessageWithType {
//    encoder: MessageEncoder,
//}
//
//impl OutgoingMessageWithType {
//    pub(crate) fn message_type(mut self, message_type: MessageType) -> Result<OutgoingMessageWithSequenceNumber> {
//        self.encoder.message_type(message_type)?;
//        Ok(OutgoingMessageWithSequenceNumber { encoder: self.encoder })
//    }
//}
//
//struct OutgoingMessageWithSequenceNumber {
//    encoder: MessageEncoder,
//}
//
//impl OutgoingMessageWithSequenceNumber {
//    pub(crate) fn sequence_number(mut self, sequence_number: u64) -> Result<OutgoingMessageWithNotifications> {
//        self.encoder.sequence_number(sequence_number)?;
//        Ok(OutgoingMessageWithNotifications { encoder: self.encoder })
//    }
//}
//
//struct OutgoingMessageWithNotifications {
//    encoder: MessageEncoder,
//}
//
//impl OutgoingMessageWithNotifications {
//    pub(crate) fn notifications(mut self, notifications: &[Notification]) -> Result<OutgoingMessageWithBroadcast> {
//        self.encoder.message.num_notifications = self.encoder.notifications(notifications)?;
//        Ok(OutgoingMessageWithBroadcast { encoder: self.encoder })
//    }
//}
//
//struct OutgoingMessageWithBroadcast {
//    encoder: MessageEncoder,
//}
//
//impl OutgoingMessageWithBroadcast {
//    pub(crate) fn broadcast(&mut self, members: &[Member]) -> Result<()> {
//        self.encoder.message.num_broadcast = self.encoder.broadcast(members)?;
//        Ok(())
//    }
//
//    pub(crate) fn build(mut self) -> Bytes {
//        self.encoder.message.buffer.freeze()
//    }
//}

pub(crate) struct MessageEncoder {
    message: OutgoingMessage,
}

impl MessageEncoder {
    pub(crate) fn new(max_size: usize) -> Self {
        MessageEncoder {
            message: OutgoingMessage {
                buffer: BytesMut::with_capacity(max_size),
                num_notifications: 0,
                num_broadcast: 0,
            },
        }
    }

    pub(crate) fn message_type(mut self, message_type: MessageType) -> Result<Self> {
        if self.message.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.message.buffer.put_i32_be(message_type as i32);
        Ok(self)
    }

    pub(crate) fn sequence_number(mut self, sequence_number: u64) -> Result<Self> {
        if self.message.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.message.buffer.put_u64_be(sequence_number);
        Ok(self)
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

    pub(crate) fn notifications(mut self, notifications: &[Notification]) -> Result<Self> {
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
        self.message.num_notifications = count as usize;
        Ok(self)
    }

    pub(crate) fn broadcast(mut self, members: &[Member]) -> Result<Self> {
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
        self.message.num_broadcast = count as usize;
        Ok(self)
    }

    pub(crate) fn encode(self) -> OutgoingMessageEnum {
        OutgoingMessageEnum::DisseminationMessage(self.message)
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
