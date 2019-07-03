use bytes::{BufMut, Buf, BytesMut};
use std::net::{IpAddr, SocketAddr, Ipv4Addr};
use std::io::Cursor;
use std::fmt;

#[derive(Debug, PartialEq)]
pub(super) enum MessageType {
    Ping,
    PingAck,
    PingIndirect,
}

pub(super) struct Message {
    buffer: BytesMut,
}

impl Message {
    pub(super) fn create(message_type: MessageType, sequence_number: u64, epoch: u64) -> Self {
        let mut message = Message{ buffer: BytesMut::with_capacity(64) };
        message.buffer.put_i32_be(message_type as i32);
        message.buffer.put_u64_be(sequence_number);
        message.buffer.put_u64_be(epoch);
        message
    }

    pub(super) fn with_members(&mut self, alive: &[SocketAddr], dead: &[SocketAddr]) -> (usize, usize) {
        let count_alive = self.add_members(alive);
        let count_dead = self.add_members(dead);
        (count_alive, count_dead)
    }

    fn add_members(&mut self, members: &[SocketAddr]) -> usize {
        // 00101001 -> 5 addresses (highest 1 defines when the address types start): v4, v6, v4, v4, v6
        let header_position = self.buffer.len();
        self.buffer.resize(self.buffer.len()+1, 0u8); // leave a byte for header
        let mut header = 0u8;
        let count = std::cmp::min(members.len(), std::mem::size_of_val(&header)*8-1);
        for idx in 0..count {
            match members[idx] {
                SocketAddr::V4(sa) => {
                    self.buffer.put_slice(&(sa.ip().octets()));
                    self.buffer.put_u16_be(sa.port());
                }
                SocketAddr::V6(_sa) => {
                    panic!("IPv6 is not implemented yet.");
//                    header |= 1 << idx;
                }
            }
        }
        header |= 1 << count as u8;
        *self.buffer.iter_mut().skip(header_position).next().unwrap() = header;
        count
    }

    pub(super) fn get_type(&self) -> MessageType {
        let encoded_type = self.get_cursor_into_buffer(0).get_i32_be();
        match encoded_type {
            x if x == MessageType::Ping as i32 => {
                MessageType::Ping
            }
            x if x == MessageType::PingAck as i32 => {
                MessageType::PingAck
            }
            x if x == MessageType::PingIndirect as i32 => {
                MessageType::PingIndirect
            }
            _ => {
                panic!("No such message type")
            }
        }
    }

    pub(super) fn get_sequence_number(&self) -> u64 {
        self.get_cursor_into_buffer(std::mem::size_of::<i32>() as u64).get_u64_be()
    }

    pub(super) fn get_epoch(&self) -> u64 {
        self.get_cursor_into_buffer(
            (std::mem::size_of::<i32>() + std::mem::size_of::<u64>()) as u64
        ).get_u64_be()
    }

    pub(super) fn get_alive_members(&self) -> Vec<SocketAddr> {
        self.get_members((std::mem::size_of::<i32>() + std::mem::size_of::<u64>() * 2) as u64)
    }

    pub(super) fn get_dead_members(&self) -> Vec<SocketAddr> {
        let alive_position = (std::mem::size_of::<i32>() + std::mem::size_of::<u64>() * 2) as u64;
        let position = alive_position + std::mem::size_of::<u8>() as u64 + self.count_members_bytes(alive_position);
        self.get_members(position)
    }

    pub(super) fn get_members(&self, position: u64) -> Vec<SocketAddr> {
        let mut cursor = self.get_cursor_into_buffer(position);
        let header = cursor.get_u8();
        let count = std::mem::size_of_val(&header) * 8 - header.leading_zeros() as usize - 1;
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            if (header & 1) == 0 {
                result.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(cursor.get_u32_be())), cursor.get_u16_be()));
            }
            else {
                panic!("IPv6 is not implemented yet.");
            }
            let _ = header >> 1;
        }
        result
    }

    fn count_members_bytes(&self, position: u64) -> u64 {
        let mut cursor = self.get_cursor_into_buffer(position);
        let header = cursor.get_u8();
        let count = std::mem::size_of_val(&header) * 8 - header.leading_zeros() as usize - 1;
        let mut result: usize = 0;
        for _ in 0..count {
            if (header & 1) == 0 {
                result += std::mem::size_of::<u32>() + std::mem::size_of::<u16>();
            } else {
                panic!("IPv6 is not implemented yet.")
            }
        }
        result as u64
    }

    pub(super) fn into_inner(self) -> BytesMut {
        self.buffer
    }

    fn get_cursor_into_buffer(&self, position: u64) -> Cursor<&BytesMut> {
        let mut cursor = Cursor::new(&self.buffer);
        cursor.set_position(position);
        cursor
    }
}

impl From<&[u8; 64]> for  Message {
    fn from(src: &[u8; 64]) -> Self {
        Message{ buffer: bytes::BytesMut::from(&src[..]) }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Message {{ type: {:?}, epoch: {}, sequence_number: {}, alive: {:?}, dead: {:?} }}",
               self.get_type(), self.get_epoch(), self.get_sequence_number(), self.get_alive_members(), self.get_dead_members()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;
    use std::net::SocketAddrV4;

    #[test]
    fn create_message() {
        let message = Message::create(MessageType::Ping, 1, 2);
        assert_eq!(message.get_type(), MessageType::Ping);
        assert_eq!(message.get_sequence_number(), 1);
        assert_eq!(message.get_epoch(), 2);
    }

    #[test]
    fn add_members() {
        let mut message = Message::create(MessageType::Ping, 1, 2);

        message.with_members(&[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())], &[]);
        assert_eq!(message.get_alive_members(), [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]);
        assert_eq!(message.get_dead_members(), []);

        let mut message = Message::create(MessageType::Ping, 1, 2);
        message.with_members(&[], &[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]);
        assert_eq!(message.get_alive_members(), []);
        assert_eq!(message.get_dead_members(), [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]);

        let mut message = Message::create(MessageType::Ping, 1, 2);
        message.with_members(
            &[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())],
            &[SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())]
        );
        assert_eq!(message.get_alive_members(), [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]);
        assert_eq!(message.get_dead_members(), [SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())]);

        let mut message = Message::create(MessageType::Ping, 1, 2);
        message.with_members(
            &[
                SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap()),
                SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())
            ],
            &[]
        );
        assert_eq!(
            message.get_alive_members(),
            [
                SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap()),
                SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())
            ]
        );
        assert_eq!(message.get_dead_members(), []);
    }
}
