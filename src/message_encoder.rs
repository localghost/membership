use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::buf::ext::{BufMutExt, Limit};
use bytes::{BufMut, Bytes, BytesMut};
use failure::_core::marker::PhantomData;
use failure::format_err;
use std::net::SocketAddr;

macro_rules! size_of_vals {
    ($x:expr) => {
        std::mem::size_of_val(&$x)
    };
    ($x:expr, $($y:expr),+) => {
        (size_of_vals!($x) + size_of_vals!($($y),+))
    };
}

pub(crate) struct PingRequestMessageEncoder {
    buffer: Limit<BytesMut>,
}

impl PingRequestMessageEncoder {
    pub(crate) fn new() -> SenderEncoder<SequenceNumberEncoder<TargetEncoder<Self>>> {
        // TODO: calculate exact max length as capacity
        let mut buffer = BytesMut::with_capacity(1024).limit(1024);
        buffer.put_i32(MessageType::PingIndirect as i32);
        SenderEncoder::<SequenceNumberEncoder<TargetEncoder<Self>>>::from(buffer)
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::PingRequestMessage(PingRequestMessageOut {
            buffer: self.buffer.into_inner().freeze(),
        })
    }
}

impl From<Limit<BytesMut>> for PingRequestMessageEncoder {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self { buffer }
    }
}

pub(crate) struct DisseminationMessageEncoder {}

impl DisseminationMessageEncoder {
    pub(crate) fn new(
        max_size: usize,
    ) -> MessageTypeEncoder<SenderEncoder<SequenceNumberEncoder<NotificationsEncoder>>> {
        MessageTypeEncoder::<SenderEncoder<SequenceNumberEncoder<NotificationsEncoder>>> {
            buffer: BytesMut::with_capacity(max_size).limit(max_size),
            phantom: PhantomData,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PingRequestMessageOut {
    buffer: Bytes,
}

#[derive(Debug)]
pub(crate) struct DisseminationMessageOut {
    buffer: Bytes,
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

pub(crate) struct SenderEncoder<T> {
    buffer: Limit<BytesMut>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> SenderEncoder<T>
where
    T: From<Limit<BytesMut>>,
{
    pub(crate) fn sender(mut self, member: &Member) -> Result<T> {
        encode_member(member, &mut self.buffer)?;
        Ok(T::from(self.buffer))
    }
}

impl<T> From<Limit<BytesMut>> for SenderEncoder<T> {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self {
            buffer,
            phantom: PhantomData,
        }
    }
}

pub(crate) struct TargetEncoder<T> {
    buffer: Limit<BytesMut>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> TargetEncoder<T>
where
    T: From<Limit<BytesMut>>,
{
    pub(crate) fn target(mut self, member: &Member) -> Result<T> {
        encode_member(member, &mut self.buffer)?;
        Ok(T::from(self.buffer))
    }
}

impl<T> From<Limit<BytesMut>> for TargetEncoder<T> {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self {
            buffer,
            phantom: PhantomData,
        }
    }
}

pub(crate) struct MessageTypeEncoder<T> {
    buffer: Limit<BytesMut>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> MessageTypeEncoder<T>
where
    T: From<Limit<BytesMut>>,
{
    pub(crate) fn message_type(mut self, message_type: MessageType) -> Result<T> {
        if self.buffer.remaining_mut() < std::mem::size_of::<i32>() {
            return Err(format_err!("Could not encode message type"));
        }
        self.buffer.put_i32(message_type as i32);
        Ok(T::from(self.buffer))
    }
}

impl<T> From<Limit<BytesMut>> for MessageTypeEncoder<T> {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self {
            buffer,
            phantom: PhantomData,
        }
    }
}

pub(crate) struct SequenceNumberEncoder<T> {
    buffer: Limit<BytesMut>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> SequenceNumberEncoder<T>
where
    T: From<Limit<BytesMut>>,
{
    pub(crate) fn sequence_number(mut self, sequence_number: u64) -> Result<T> {
        if self.buffer.remaining_mut() < size_of_vals!(sequence_number) {
            return Err(format_err!("Could not encode sequence number"));
        }
        self.buffer.put_u64(sequence_number);
        Ok(T::from(self.buffer))
    }
}

impl<T> From<Limit<BytesMut>> for SequenceNumberEncoder<T> {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self {
            buffer,
            phantom: PhantomData,
        }
    }
}

pub(crate) struct NotificationsEncoder {
    buffer: Limit<BytesMut>,
    num_notifications: usize,
}

impl NotificationsEncoder {
    pub(crate) fn notifications<'a>(
        mut self,
        notifications: impl Iterator<Item = &'a Notification>,
    ) -> Result<BroadcastEncoder> {
        if self.buffer.has_remaining_mut() {
            let count_position = self.buffer.get_ref().len();
            self.buffer.put_u8(0);
            let mut count = 0;
            for notification in notifications {
                if Self::size_of_notification(notification) > self.buffer.remaining_mut() {
                    break;
                }
                self.encode_notification(notification)?;
                count += 1;
            }
            self.buffer.get_mut()[count_position] = count;
            self.num_notifications = count as usize;
        }
        Ok(BroadcastEncoder {
            buffer: self.buffer,
            num_notifications: self.num_notifications,
            num_broadcast: 0,
        })
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::DisseminationMessage(DisseminationMessageOut {
            buffer: self.buffer.into_inner().freeze(),
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

    fn size_of_notification(notification: &Notification) -> usize {
        std::mem::size_of::<u8>()
            + match notification {
                Notification::Alive { member }
                | Notification::Suspect { member }
                | Notification::Confirm { member } => size_of_member(member),
            }
    }
}

impl From<Limit<BytesMut>> for NotificationsEncoder {
    fn from(buffer: Limit<BytesMut>) -> Self {
        Self {
            buffer,
            num_notifications: 0,
        }
    }
}

pub(crate) struct BroadcastEncoder {
    buffer: Limit<BytesMut>,
    num_notifications: usize,
    num_broadcast: usize,
}

impl BroadcastEncoder {
    pub(crate) fn broadcast<'a>(mut self, members: impl Iterator<Item = &'a Member>) -> Result<Self> {
        if !self.buffer.has_remaining_mut() {
            return Err(format_err!("Not enough space to encode broadcast"));
        }
        let count_position = self.buffer.get_ref().len();
        self.buffer.put_u8(0);
        let mut count = 0;
        for member in members {
            encode_member(member, &mut self.buffer)?;
            count += 1;
        }
        self.buffer.get_mut()[count_position] = count;
        self.num_broadcast = count as usize;
        Ok(self)
    }

    pub(crate) fn encode(self) -> OutgoingMessage {
        OutgoingMessage::DisseminationMessage(DisseminationMessageOut {
            buffer: self.buffer.into_inner().freeze(),
            num_notifications: self.num_notifications,
            num_broadcast: self.num_broadcast,
        })
    }
}

fn encode_member(member: &Member, buffer: &mut Limit<BytesMut>) -> Result<()> {
    if buffer.remaining_mut() < size_of_member(member) {
        return Err(format_err!("Could not encode member"));
    }
    let position = buffer.get_ref().len();
    buffer.put_u8(0u8);
    buffer.put_slice(member.id.as_slice());
    buffer.put_u64(member.incarnation);
    match member.address {
        SocketAddr::V4(address) => {
            buffer.put_slice(&address.ip().octets());
            buffer.put_u16(address.port());
        }
        SocketAddr::V6(_) => {
            buffer.get_mut()[position] = 1u8 << 7;
            todo!();
        }
    };
    Ok(())
}

fn size_of_member(member: &Member) -> usize {
    std::mem::size_of::<u8>()
        + size_of_vals!(member.id, member.incarnation)
        + match member.address {
            SocketAddr::V4(address) => size_of_vals!(address.ip().octets(), address.port()),
            SocketAddr::V6(_) => todo!(),
        }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    struct NullEncoder {
        buffer: Limit<BytesMut>,
    }
    impl From<Limit<BytesMut>> for NullEncoder {
        fn from(buffer: Limit<BytesMut>) -> Self {
            Self { buffer }
        }
    }

    #[test]
    fn should_not_encode_notifications_into_empty_buffer() {
        let notifications = vec![Notification::Alive {
            member: Member::new(SocketAddr::from_str("127.0.0.1:1234").unwrap()),
        }];
        let encoder = NotificationsEncoder::from(BytesMut::with_capacity(0).limit(0));
        encoder.notifications(notifications.iter()).unwrap();
    }

    #[test]
    fn should_encode_notifications_without_overflowing_buffer() {
        let notifications = vec![Notification::Alive {
            member: Member::new(SocketAddr::from_str("127.0.0.1:1234").unwrap()),
        }];
        let encoder = NotificationsEncoder::from(BytesMut::with_capacity(1).limit(1));
        let encoder = encoder.notifications(notifications.iter()).unwrap();
        assert_eq!(encoder.num_notifications, 0);
    }
}
