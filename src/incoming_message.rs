#![deny(missing_docs)]

use crate::member::Member;
use crate::notification::Notification;
use std::net::SocketAddr;

#[derive(Debug)]
pub(crate) struct DisseminationMessage {
    pub(crate) sequence_number: u64,
    pub(crate) notifications: Vec<Notification>,
    pub(crate) broadcast: Vec<Member>,
}

#[derive(Debug)]
pub(crate) struct PingRequestMessage {
    pub(crate) sequence_number: u64,
    pub(crate) target: SocketAddr,
}

#[derive(Debug)]
pub(crate) enum IncomingMessage {
    Ping(DisseminationMessage),
    Ack(DisseminationMessage),
    PingRequest(PingRequestMessage),
}

impl From<IncomingMessage> for DisseminationMessage {
    fn from(im: IncomingMessage) -> Self {
        match im {
            IncomingMessage::Ping(message) | IncomingMessage::Ack(message) => message,
            _ => unreachable!(),
        }
    }
}

impl From<IncomingMessage> for PingRequestMessage {
    fn from(im: IncomingMessage) -> Self {
        match im {
            IncomingMessage::PingRequest(message) => message,
            _ => unreachable!(),
        }
    }
}
