#![deny(missing_docs)]

use crate::member::Member;
use crate::notification::Notification;
use std::net::SocketAddr;

pub(crate) enum ProtocolMessage {
    Ping {
        sequence_number: u64,
        notifications: Vec<Notification>,
        broadcast: Vec<Member>,
    },
    Ack {
        sequence_number: u64,
        notifications: Vec<Notification>,
        broadcast: Vec<Member>,
    },
    PingRequestMessage {
        sequence_number: u64,
        target: SocketAddr,
    },
}
