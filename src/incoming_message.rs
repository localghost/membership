#![deny(missing_docs)]

use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;

#[derive(Debug)]
pub(crate) struct IncomingMessage {
    pub(crate) message_type: MessageType,
    pub(crate) sequence_number: u64,
    pub(crate) notifications: Vec<Notification>,
    pub(crate) broadcast: Vec<Member>,
}
