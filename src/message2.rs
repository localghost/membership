#![deny(missing_docs)]

use crate::message::MessageType;
use crate::notification::Notification;

pub(crate) struct Message {
    pub(crate) message_type: MessageType,
    pub(crate) sequence_number: u64,
    pub(crate) notifications: Vec<Notification>,
}
