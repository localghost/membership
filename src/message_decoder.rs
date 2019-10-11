use crate::incoming_message::IncomingMessage;
use crate::member::Member;
use crate::message::MessageType;
use crate::notification::Notification;
use crate::result::Result;
use bytes::Buf;
use failure::format_err;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub struct MessageDecoder<'a> {
    buffer: Cursor<&'a [u8]>,
}

impl<'a> MessageDecoder<'a> {
    pub(crate) fn decode(buffer: &[u8]) -> Result<IncomingMessage> {
        MessageDecoder {
            buffer: Cursor::new(buffer),
        }
        .decode_message()
    }

    fn decode_message(&mut self) -> Result<IncomingMessage> {
        Ok(IncomingMessage {
            message_type: self.decode_message_type()?,
            sequence_number: self.decode_sequence_number()?,
            notifications: self.decode_notifications()?,
            broadcast: self.decode_broadcast()?,
        })
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
        // [7][6|5|4][3|2|1|0]
        // +-----------------+
        // 0-3: notification type
        // 4-6: reserved
        // 7: IP address type
        let header = self.buffer.get_u8();
        let member = self.decode_member(header >> 7)?;
        let notification = match header & 0x0f {
            0 => Notification::Alive { member },
            1 => Notification::Suspect { member },
            2 => Notification::Confirm { member },
            x => return Err(format_err!("Unsupported notification: {}", x)),
        };
        Ok(notification)
    }

    fn decode_member(&mut self, address_type: u8) -> Result<Member> {
        // FIXME: The first value should depend on the `address_type`.
        if self.buffer.remaining()
            < (std::mem::size_of::<u32>() + std::mem::size_of::<u16>() + std::mem::size_of::<u64>())
        {
            return Err(format_err!("Could not decode member"));
        }

        let address = match address_type {
            0 => SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(self.buffer.get_u32_be())),
                self.buffer.get_u16_be(),
            ),
            1 => return Err(format_err!("Support for IPv6 is not implemented yet")),
            x => return Err(format_err!("Unsupported address type: {}", x)),
        };

        Ok(Member {
            address,
            incarnation: self.buffer.get_u64_be(),
        })
    }

    fn decode_broadcast(&mut self) -> Result<Vec<Member>> {
        if !self.buffer.has_remaining() {
            return Ok(Vec::new());
        }
        let count = self.buffer.get_u8();
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            // TODO read IP address type
            result.push(self.decode_member()?);
        }
        Ok(result)
    }
}

fn decode_message(buffer: &[u8]) -> Result<IncomingMessage> {
    // 1. check protocol version in buffer
    // 2. create proper decoder
    // 3. decode and return message
    MessageDecoder::decode(buffer)
}
