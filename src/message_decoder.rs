use crate::incoming_message::{DisseminationMessageIn, IncomingMessage, PingRequestMessageIn};
use crate::member::{Id as MemberId, Member};
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::Buf;
use failure::format_err;
use std::convert::TryFrom;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

struct MessageDecoder<'a> {
    buffer: Cursor<&'a [u8]>,
}

impl<'a> MessageDecoder<'a> {
    fn decode(buffer: &[u8]) -> Result<IncomingMessage> {
        MessageDecoder {
            buffer: Cursor::new(buffer),
        }
        .decode_message()
    }

    fn decode_message(&mut self) -> Result<IncomingMessage> {
        let message_type = self.decode_message_type()?;
        let message = match message_type {
            MessageType::Ping => IncomingMessage::Ping(DisseminationMessageIn {
                sender: self.decode_sender()?,
                sequence_number: self.decode_sequence_number()?,
                notifications: self.decode_notifications()?,
                broadcast: self.decode_broadcast()?,
            }),
            MessageType::PingAck => IncomingMessage::Ack(DisseminationMessageIn {
                sender: self.decode_sender()?,
                sequence_number: self.decode_sequence_number()?,
                notifications: self.decode_notifications()?,
                broadcast: self.decode_broadcast()?,
            }),
            MessageType::PingIndirect => IncomingMessage::PingRequest(PingRequestMessageIn {
                sender: self.decode_sender()?,
                sequence_number: self.decode_sequence_number()?,
                target: self.decode_target()?,
            }),
        };
        Ok(message)
    }

    fn decode_message_type(&mut self) -> Result<MessageType> {
        let message_type = self.buffer.get_i32_be();
        match message_type {
            x if x == MessageType::Ping as i32 => Ok(MessageType::Ping),
            x if x == MessageType::PingAck as i32 => Ok(MessageType::PingAck),
            x if x == MessageType::PingIndirect as i32 => Ok(MessageType::PingIndirect),
            x => Err(format_err!("Unsupported message type: {}", x)),
        }
    }

    fn decode_sequence_number(&mut self) -> Result<u64> {
        if self.buffer.remaining() < std::mem::size_of::<u64>() {
            return Err(format_err!("Not enough bytes to discover message sequence number"));
        }
        Ok(self.buffer.get_u64_be())
    }

    fn decode_notifications(&mut self) -> Result<Vec<Notification>> {
        if !self.buffer.has_remaining() {
            return Ok(Vec::new());
        }

        let count = self.buffer.get_u8();
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            result.push(self.decode_notification()?);
        }
        Ok(result)
    }

    fn decode_notification(&mut self) -> Result<Notification> {
        if !self.buffer.has_remaining() {
            return Err(format_err!("Unable to decode notification header"));
        }
        // Notification header:
        // +-----------------+
        // [7|6|5|4][3|2|1|0]
        // +-----------------+
        // 0-3: notification type
        // 4-7: reserved
        let header = self.buffer.get_u8();
        let member = self.decode_member()?;
        let notification = match header & 0x0f {
            0 => Notification::Alive { member },
            1 => Notification::Suspect { member },
            2 => Notification::Confirm { member },
            x => return Err(format_err!("Unsupported notification: {}", x)),
        };
        Ok(notification)
    }

    fn decode_sender(&mut self) -> Result<Member> {
        if !self.buffer.has_remaining() {
            return Err(format_err!("Could not decode sender"));
        }
        self.decode_member()
    }

    fn decode_member(&mut self) -> Result<Member> {
        let address_type = self.buffer.get_u8();
        let member_id = self.decode_member_id()?;
        if self.buffer.remaining() < std::mem::size_of::<u64>() {
            return Err(format_err!("Could not decode member"));
        }
        let incarnation = self.buffer.get_u64_be();
        let address = self.decode_address(address_type)?;
        Ok(Member {
            id: member_id,
            address,
            incarnation,
        })
    }

    fn decode_member_id(&mut self) -> Result<MemberId> {
        if self.buffer.remaining() < std::mem::size_of::<MemberId>() {
            return Err(format_err!("Could not decode member id"));
        }
        let member_id = MemberId::try_from(
            &self.buffer.get_ref()
                [self.buffer.position() as usize..(self.buffer.position() as usize + std::mem::size_of::<MemberId>())],
        )?;
        self.buffer.advance(std::mem::size_of::<MemberId>());
        Ok(member_id)
    }

    fn decode_address(&mut self, address_type: u8) -> Result<SocketAddr> {
        // FIXME: The first value should depend on the `address_type`.
        if self.buffer.remaining() < (std::mem::size_of::<u32>() + std::mem::size_of::<u16>()) {
            return Err(format_err!("Could not decode member address"));
        }
        let address = match address_type {
            0 => SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(
                    self.buffer.get_u8(),
                    self.buffer.get_u8(),
                    self.buffer.get_u8(),
                    self.buffer.get_u8(),
                )),
                self.buffer.get_u16_be(),
            ),
            1 => return Err(format_err!("Support for IPv6 is not implemented yet")),
            x => return Err(format_err!("Unsupported address type: {}", x)),
        };
        Ok(address)
    }

    fn decode_broadcast(&mut self) -> Result<Vec<Member>> {
        if !self.buffer.has_remaining() {
            return Ok(Vec::new());
        }
        let count = self.buffer.get_u8();
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            result.push(self.decode_member()?);
        }
        Ok(result)
    }

    fn decode_target(&mut self) -> Result<Member> {
        if !self.buffer.has_remaining() {
            return Err(format_err!("Could not decode ping request target"));
        }
        self.decode_member()
    }
}

pub(crate) fn decode_message(buffer: &[u8]) -> Result<IncomingMessage> {
    // 1. check protocol version in buffer
    // 2. create proper decoder
    MessageDecoder::decode(buffer)
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::str::FromStr;

    //    #[test]
    //    fn decode_empty_message() {
    //        let mut buffer = BytesMut::new();
    //        buffer.put_i32_be(MessageType::Ping as i32);
    //        buffer.put_u64_be(42);
    //
    //        //        let message = ;
    //        match decode_message(&buffer).unwrap() {
    //            IncomingMessage::Ping(message) => assert_eq!(message.sequence_number, 42),
    //            _ => assert!(false),
    //        }
    //        //        if let IncomingMessage::Ping(message) =  {
    //        //        } else {
    //        //            assert
    //        //        }
    //        //        assert_eq!(message.message_type, MessageType::Ping);
    //        //        assert_eq!(message.sequence_number, 42);
    //        //        assert!(message.notifications.is_empty());
    //        //        assert!(message.broadcast.is_empty());
    //    }
    //
    //    #[test]
    //    fn decode_message_with_notifications() {
    //        let mut buffer = BytesMut::new();
    //        buffer.put_i32_be(MessageType::Ping as i32);
    //        buffer.put_u64_be(42);
    //        buffer.put_u8(1);
    //        buffer.put_u8(0);
    //        buffer.put_slice(&Ipv4Addr::from_str("127.0.0.1").unwrap().octets());
    //        buffer.put_u16_be(4567);
    //        buffer.put_u64_be(123);
    //
    //        let message = decode_message(&buffer).unwrap();
    //        assert_eq!(message.message_type, MessageType::Ping);
    //        assert_eq!(message.sequence_number, 42);
    //        assert!(message.broadcast.is_empty());
    //        assert_eq!(1, message.notifications.len());
    //
    //        let notification = message.notifications.get(0).unwrap();
    //        if let Notification::Alive { member } = notification {
    //            assert!(member.address.is_ipv4());
    //            assert_eq!("127.0.0.1:4567", member.address.to_string());
    //            assert_eq!(123, member.incarnation);
    //        } else {
    //            assert!(false, "Not an Alive notification.");
    //        }
    //    }
    //
    //    #[test]
    //    fn decode_message_with_broadcast() {
    //        // TODO
    //    }

    #[test]
    fn decode_encoded_message() -> Result<()> {
        use crate::message_encoder::DisseminationMessageEncoder;

        let sender = Member::new(SocketAddr::from_str("127.0.0.1:2345")?);
        let notifications = vec![
            Notification::Alive {
                member: Member::new(SocketAddr::from_str("127.0.1.1:5432")?),
            },
            Notification::Suspect {
                member: Member::new(SocketAddr::from_str("127.0.1.2:5432")?),
            },
        ];
        let broadcast = vec![
            Member::new(SocketAddr::from_str("127.0.1.1:5432")?),
            Member::new(SocketAddr::from_str("127.0.1.2:5432")?),
        ];
        let encoded_message = DisseminationMessageEncoder::new(1024)
            .message_type(MessageType::Ping)?
            .sender(&sender)?
            .sequence_number(24)?
            .notifications(notifications.iter())?
            .broadcast(broadcast.iter())?
            .encode();

        let decoded_message = decode_message(encoded_message.buffer())?;

        if let IncomingMessage::Ping(ping_message) = decoded_message {
            assert_eq!(ping_message.sender, sender);
            assert_eq!(ping_message.sequence_number, 24);
            assert_eq!(ping_message.notifications, notifications);
            assert_eq!(ping_message.broadcast, broadcast);
        } else {
            assert!(false, "Not a Ping message");
        }

        Ok(())
    }
}
