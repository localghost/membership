use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::{BufMut, Bytes, BytesMut};
use failure::format_err;
use std::net::SocketAddr;

//pub(crate) struct SenderEncoder2<T> {
//    buffer: BytesMut,
//    phantom: std::marker::PhantomData<T>,
//}
//
//impl<T> SenderEncoder2<T>
//where
//    T: From<BytesMut>,
//{
//    pub(crate) fn sender(mut self, member: &Member) -> Result<T> {
//        encode_member(member, &mut self.buffer);
//        Ok(T::from(self.buffer))
//    }
//}

macro_rules! size_of_vals {
    ($x:expr) => {
        std::mem::size_of_val(&$x)
    };
    ($x:expr, $($y:expr),+) => {
        (size_of_vals!($x) + size_of_vals!($($y),+))
    };
}

#[derive(Debug)]
pub(crate) struct PingRequestMessageOut {
    buffer: Bytes,
}

#[derive(Debug)]
pub(crate) struct DisseminationMessageOut {
    buffer: BytesMut,
    num_notifications: usize,
    num_broadcast: usize,
}

impl DisseminationMessageOut {
    pub(crate) fn num_notifications(&self) -> usize {
        self.num_notifications
    }

    pub(crate) fn num_broadcast(&self) -> usize {
        self.num_broadcast
    }
}

#[derive(Debug)]
pub(crate) enum OutgoingMessage {
    DisseminationMessage(DisseminationMessageOut),
    PingRequestMessage(PingRequestMessageOut),
}

impl OutgoingMessage {
    pub(crate) fn buffer(&self) -> &[u8] {
        match self {
            OutgoingMessage::DisseminationMessage(ref message) => &message.buffer,
            OutgoingMessage::PingRequestMessage(ref message) => &message.buffer,
        }
    }
}

fn encode_member(member: &Member, buffer: &mut BytesMut) -> Result<()> {
    if buffer.remaining_mut() < (std::mem::size_of::<u8>() + size_of_vals!(member.id, member.incarnation)) {
        return Err(format_err!("Could not encode member"));
    }

    let position = buffer.len();
    buffer.put_u8(0u8);
    buffer.put_slice(member.id.as_slice());
    buffer.put_u64_be(member.incarnation);
    match member.address {
        SocketAddr::V4(address) => {
            if buffer.remaining_mut() < size_of_vals!(address.ip().octets(), address.port()) {
                return Err(format_err!("Could not encode member address"));
            }
            buffer.put_slice(&address.ip().octets());
            buffer.put_u16_be(address.port());
        }
        SocketAddr::V6(_) => {
            buffer[position] = 1u8 << 7;
            todo!();
        }
    };
    Ok(())
}

//pub(crate) fn encode_message_ping_request(
//    sender: &Member,
//    sequence_number: u64,
//    target: &Member,
//) -> PingRequestMessageOut {
//    let mut buffer = BytesMut::new();
//    // TODO: Header should contain IP type for `sender` and `target`, and number of bytes for Member Id.
//    let mut header = 0u8;
//    if sender.address.is_ipv6() {
//        header |= 1u8 << 7;
//    }
//    if target.address.is_ipv6() {
//        header |= 1u8 << 6;
//    }
//    buffer.put_u8(header);
//    encode_member(sender, &mut buffer);
//    buffer.put_u64_be(sequence_number);
//    encode_member(target, &mut buffer);
//
//    PingRequestMessageOut {
//        buffer: buffer.freeze(),
//    }
//}

pub(crate) struct PingRequestMessageEncoder {
    buffer: BytesMut,
}

impl PingRequestMessageEncoder {
    pub(crate) fn new() -> Self {
        // TODO: calculate exact max length as capacity
        let mut encoder = PingRequestMessageEncoder {
            buffer: BytesMut::with_capacity(1024),
        };
        encoder.buffer.put_i32_be(MessageType::PingIndirect as i32);
        encoder
    }

    pub(crate) fn sender(mut self, member: &Member) -> Result<Self> {
        encode_member(member, &mut self.buffer)?;
        Ok(self)
    }

    pub(crate) fn sequence_number(mut self, sequence_number: u64) -> Result<Self> {
        self.buffer.put_u64_be(sequence_number);
        Ok(self)
    }

    pub(crate) fn target(mut self, member: &Member) -> Result<Self> {
        encode_member(member, &mut self.buffer)?;
        Ok(self)
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::PingRequestMessage(PingRequestMessageOut {
            buffer: self.buffer.freeze(),
        })
    }
}

pub(crate) struct MessageTypeEncoder {
    buffer: BytesMut,
}

impl MessageTypeEncoder {
    pub(crate) fn message_type(mut self, message_type: MessageType) -> Result<SenderEncoder> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.buffer.put_i32_be(message_type as i32);
        Ok(SenderEncoder { buffer: self.buffer })
        //        Ok(SenderEncoder2 {
        //            buffer: self.encoder.message.buffer,
        //            phantom: PhantomData,
        //        })
    }
}

pub(crate) struct SenderEncoder {
    buffer: BytesMut,
}

impl SenderEncoder {
    pub(crate) fn sender(mut self, member: &Member) -> Result<SequenceNumberEncoder> {
        encode_member(member, &mut self.buffer)?;
        Ok(SequenceNumberEncoder { buffer: self.buffer })
    }
}

pub(crate) struct SequenceNumberEncoder {
    buffer: BytesMut,
}

impl SequenceNumberEncoder {
    pub(crate) fn sequence_number(mut self, sequence_number: u64) -> Result<NotificationsEncoder> {
        if self.buffer.remaining_mut() < size_of_vals!(sequence_number) {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.buffer.put_u64_be(sequence_number);
        Ok(NotificationsEncoder {
            buffer: self.buffer,
            num_notifications: 0,
        })
    }
}

//impl From<BytesMut> for SequenceNumberEncoder {
//    fn from(_: BytesMut) -> Self {
//        Self {
//            encoder: DisseminationMessageEncoder::new(1024).encoder,
//        }
//    }
//}

pub(crate) struct NotificationsEncoder {
    buffer: BytesMut,
    num_notifications: usize,
}

impl NotificationsEncoder {
    pub(crate) fn notifications<'a>(
        mut self,
        notifications: impl Iterator<Item = &'a Notification>,
    ) -> Result<BroadcastEncoder> {
        if !self.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode notifications"));
        }
        let count_position = self.buffer.len();
        self.buffer.put_u8(0);
        let mut count = 0;
        for notification in notifications {
            self.encode_notification(notification)?;
            count += 1;
        }
        self.buffer[count_position] = count;
        self.num_notifications = count as usize;
        Ok(BroadcastEncoder {
            buffer: self.buffer,
            num_notifications: self.num_notifications,
            num_broadcast: 0,
        })
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::DisseminationMessage(DisseminationMessageOut {
            buffer: self.buffer,
            num_notifications: self.num_notifications,
            num_broadcast: 0,
        })
    }

    fn encode_notification(&mut self, notification: &Notification) -> Result<()> {
        if self.buffer.remaining_mut() < 1 {
            return Err(format_err!("Could not encode notification type"));
        }
        match notification {
            Notification::Alive { member } => {
                self.buffer.put_u8(0);
                encode_member(member, &mut self.buffer)?;
            }
            Notification::Suspect { member } => {
                self.buffer.put_u8(1);
                encode_member(member, &mut self.buffer)?;
            }
            Notification::Confirm { member } => {
                self.buffer.put_u8(2);
                encode_member(member, &mut self.buffer)?;
            }
        }
        Ok(())
    }
}

pub(crate) struct BroadcastEncoder {
    buffer: BytesMut,
    num_notifications: usize,
    num_broadcast: usize,
}

impl BroadcastEncoder {
    pub(crate) fn broadcast<'a>(mut self, members: impl Iterator<Item = &'a Member>) -> Result<Self> {
        if !self.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode broadcast"));
        }
        let count_position = self.buffer.len();
        self.buffer.resize(self.buffer.len() + 1, 0u8);
        let mut count = 0;
        for member in members {
            encode_member(member, &mut self.buffer)?;
            count += 1;
        }
        self.buffer[count_position] = count;
        self.num_broadcast = count as usize;
        Ok(self)
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::DisseminationMessage(DisseminationMessageOut {
            buffer: self.buffer,
            num_notifications: self.num_notifications,
            num_broadcast: self.num_broadcast,
        })
    }
}

pub(crate) struct DisseminationMessageEncoder {}

impl DisseminationMessageEncoder {
    pub(crate) fn new(max_size: usize) -> MessageTypeEncoder {
        MessageTypeEncoder {
            buffer: BytesMut::with_capacity(max_size),
        }
    }
}
