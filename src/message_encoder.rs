use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use bytes::BytesMut;

struct MessageEncoder {
    buffer: BytesMut,
}

//impl MessageEncoder {
//    fn message_type(message_type: MessageType) {}
//
//    fn sequence_number(sequence_number: u64) {}
//
//    fn notifications(notifications: &[Notification]) -> usize {}
//
//    fn broadcast(members: &[Member]) -> usize {}
//}
