#![deny(missing_docs)]

use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use std::net::SocketAddr;
use std::ops::Deref;

enum ProtocolMessage {
    PingMessage(PingAckMessage),
    AckMessage(PingAckMessage),
    PingRequestMessage { sequence_number: u64, target: SocketAddr },
}

#[derive(Debug)]
pub(crate) struct PingAckMessage {
    pub(crate) sequence_number: u64,
    pub(crate) notifications: Vec<Notification>,
    pub(crate) broadcast: Vec<Member>,
}

impl Deref for ProtocolMessage::PingMessage {
    type Target = PingAckMessage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct AckMessage(PingAckMessage);

pub(crate) struct PingRequestMessage {
    pub(crate) sequence_number: u64,
    pub(crate) target: SocketAddr,
}
