#![deny(missing_docs)]

use bytes::{Buf, BufMut, BytesMut};
use std::fmt;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, PartialEq, Copy, Clone)]
pub(super) enum MessageType {
    Ping,
    PingAck,
    PingIndirect,
}

pub(super) struct Message {
    buffer: BytesMut,
}

impl Message {
    pub(super) fn create(message_type: MessageType, sequence_number: u64) -> Self {
        let mut message = Message {
            buffer: BytesMut::with_capacity(64),
        };
        message.buffer.put_i32_be(message_type as i32);
        message.buffer.put_u64_be(sequence_number);
        message
    }

    pub(super) fn from_bytes(bytes: &[u8], count: usize) -> Self {
        Message {
            buffer: BytesMut::from(bytes.iter().take(count).cloned().collect::<Vec<_>>().as_slice()),
        }
    }

    pub(super) fn with_members(&mut self, alive: &[SocketAddr], dead: &[SocketAddr]) -> (usize, usize) {
        let count_alive = self.add_members(alive);
        let count_dead = if !dead.is_empty() { self.add_members(dead) } else { 0 };
        (count_alive, count_dead)
    }

    fn add_members(&mut self, members: &[SocketAddr]) -> usize {
        // 00101001 -> 5 addresses (highest 1 defines when the address types start): v4, v6, v4, v4, v6
        let header_position = self.buffer.len();
        self.buffer.resize(self.buffer.len() + 1, 0u8); // leave a byte for header
        let mut header = 0u8;
        let count = std::cmp::min(members.len(), std::mem::size_of_val(&header) * 8 - 1);
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
        let encoded_type = self.get_cursor_into_buffer(0).unwrap().get_i32_be();
        match encoded_type {
            x if x == MessageType::Ping as i32 => MessageType::Ping,
            x if x == MessageType::PingAck as i32 => MessageType::PingAck,
            x if x == MessageType::PingIndirect as i32 => MessageType::PingIndirect,
            _ => panic!("No such message type"),
        }
    }

    pub(super) fn get_sequence_number(&self) -> u64 {
        self.get_cursor_into_buffer(std::mem::size_of::<i32>() as u64)
            .unwrap()
            .get_u64_be()
    }

    pub(super) fn get_alive_members(&self) -> Vec<SocketAddr> {
        self.get_members((std::mem::size_of::<i32>() + std::mem::size_of::<u64>()) as u64)
    }

    pub(super) fn get_dead_members(&self) -> Vec<SocketAddr> {
        let alive_position = (std::mem::size_of::<i32>() + std::mem::size_of::<u64>()) as u64;
        let position = alive_position + std::mem::size_of::<u8>() as u64 + self.count_members_bytes(alive_position);
        self.get_members(position)
    }

    fn get_members(&self, position: u64) -> Vec<SocketAddr> {
        let cursor = self.get_cursor_into_buffer(position);
        if cursor.is_none() {
            return vec![];
        }
        let mut cursor = cursor.unwrap();
        let header = cursor.get_u8();
        let count = std::mem::size_of_val(&header) * 8 - header.leading_zeros() as usize - 1;
        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            if (header & 1) == 0 {
                result.push(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::from(cursor.get_u32_be())),
                    cursor.get_u16_be(),
                ));
            } else {
                panic!("IPv6 is not implemented yet.");
            }
            let _ = header >> 1;
        }
        result
    }

    fn count_members_bytes(&self, position: u64) -> u64 {
        let cursor = self.get_cursor_into_buffer(position);
        if cursor.is_none() {
            return 0;
        }

        let mut cursor = cursor.unwrap();
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

    pub(crate) fn count_alive(&self) -> usize {
        let alive_header_position = (std::mem::size_of::<i32>() + std::mem::size_of::<u64>()) as u64;
        let cursor = self.get_cursor_into_buffer(alive_header_position);
        if cursor.is_none() {
            return 0;
        }
        let mut cursor = cursor.unwrap();
        let header = cursor.get_u8();
        std::mem::size_of_val(&header) * 8 - header.leading_zeros() as usize - 1
    }

    #[allow(dead_code)]
    pub(super) fn into_inner(self) -> BytesMut {
        self.buffer
    }

    pub(super) fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    fn get_cursor_into_buffer(&self, position: u64) -> Option<Cursor<&BytesMut>> {
        if position >= self.buffer.len() as u64 {
            return None;
        }
        let mut cursor = Cursor::new(&self.buffer);
        cursor.set_position(position);
        Some(cursor)
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Message {{ type: {:?}, sequence_number: {}, alive: {:?}, dead: {:?} }}",
            self.get_type(),
            self.get_sequence_number(),
            self.get_alive_members(),
            self.get_dead_members()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::SocketAddrV4;
    use std::str::FromStr;

    #[test]
    fn create_message() {
        let message = Message::create(MessageType::Ping, 1);
        assert_eq!(message.get_type(), MessageType::Ping);
        assert_eq!(message.get_sequence_number(), 1);
        assert_eq!(message.get_alive_members(), []);
        assert_eq!(message.get_dead_members(), []);
        assert_eq!(message.count_alive(), 0);
    }

    #[test]
    fn count_alive() {
        let mut message = Message::create(MessageType::Ping, 1);

        message.with_members(
            &[
                SocketAddr::from_str("127.0.0.1:80").unwrap(),
                SocketAddr::from_str("127.0.0.2:8080").unwrap(),
            ],
            &[SocketAddr::from_str("192.168.0.1:80").unwrap()],
        );
        assert_eq!(message.count_alive(), 2);
    }

    #[test]
    fn add_members() {
        let mut message = Message::create(MessageType::Ping, 1);

        message.with_members(&[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())], &[]);
        assert_eq!(
            message.get_alive_members(),
            [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]
        );
        assert_eq!(message.get_dead_members(), []);

        let mut message = Message::create(MessageType::Ping, 1);
        message.with_members(&[], &[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]);
        assert_eq!(message.get_alive_members(), []);
        assert_eq!(
            message.get_dead_members(),
            [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]
        );

        let mut message = Message::create(MessageType::Ping, 1);
        message.with_members(
            &[SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())],
            &[SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())],
        );
        assert_eq!(
            message.get_alive_members(),
            [SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap())]
        );
        assert_eq!(
            message.get_dead_members(),
            [SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap())]
        );

        let mut message = Message::create(MessageType::Ping, 1);
        message.with_members(
            &[
                SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap()),
                SocketAddr::V4(SocketAddrV4::from_str("192.168.0.1:20000").unwrap()),
            ],
            &[],
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

    #[test]
    fn from_bytes() {
        let message = Message::create(MessageType::PingAck, 1);
        let buffer = message.into_inner();
        let message = Message::from_bytes(buffer.as_ref(), buffer.len());
        assert_eq!(message.get_type(), MessageType::PingAck);
        assert_eq!(message.get_sequence_number(), 1);
        assert_eq!(message.get_alive_members(), []);
        assert_eq!(message.get_dead_members(), []);
    }
}
