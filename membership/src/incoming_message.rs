#![deny(missing_docs)]

use crate::member::Member;
use crate::notification::Notification;

#[derive(Debug)]
pub(crate) struct DisseminationMessageIn {
    pub(crate) sender: Member,
    pub(crate) sequence_number: u64,
    pub(crate) notifications: Vec<Notification>,
    pub(crate) broadcast: Vec<Member>,
}

#[derive(Debug)]
pub(crate) struct PingRequestMessageIn {
    pub(crate) sender: Member,
    pub(crate) sequence_number: u64,
    pub(crate) target: Member,
}

#[derive(Debug)]
pub(crate) enum IncomingMessage {
    Ping(DisseminationMessageIn),
    Ack(DisseminationMessageIn),
    PingRequest(PingRequestMessageIn),
}

impl From<IncomingMessage> for DisseminationMessageIn {
    fn from(im: IncomingMessage) -> Self {
        match im {
            IncomingMessage::Ping(message) | IncomingMessage::Ack(message) => message,
            _ => unreachable!(),
        }
    }
}

impl From<IncomingMessage> for PingRequestMessageIn {
    fn from(im: IncomingMessage) -> Self {
        match im {
            IncomingMessage::PingRequest(message) => message,
            _ => unreachable!(),
        }
    }
}
