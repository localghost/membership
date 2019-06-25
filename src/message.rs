use bytes::{BufMut, Buf, BytesMut};
use std::net::{IpAddr, SocketAddr, Ipv4Addr, SocketAddrV4};
use std::io::Cursor;
use std::fmt;
use log::{debug, info, error};


#[derive(Debug)]
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

    pub(super) fn with_members(&mut self, members: &[SocketAddr]) -> usize {
        // 00101001 -> 5 addresses (highest 1 defines when the address types start): v4, v6, v4, v4, v6
        let header_position = self.buffer.len();
        self.buffer.resize(self.buffer.len()+1, 0u8); // leave a byte for header
        let mut header = 0u8;
        let count = std::cmp::min(members.len(), std::mem::size_of_val(&header)*8-1);
        for idx in 0..count {
            match members[idx].ip() {
                IpAddr::V4(ip) => {
                    self.buffer.put_slice(&(ip.octets()))
                }
                IpAddr::V6(_ip) => {
                    // TODO: support serializing v6
                    self.buffer.put_u8(1);
                    header |= 1 << idx;
                }
            }
        }
        header |= 1 << count;
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

    pub(super) fn get_members(&self) -> Vec<SocketAddr> {
        let mut cursor = self.get_cursor_into_buffer(
            (std::mem::size_of::<i32>() + std::mem::size_of::<u64>() * 2) as u64
        );
        let header = cursor.get_u8();
        let count = std::mem::size_of_val(&header) * 8 - header.leading_zeros() as usize - 1;
        let mut result = Vec::with_capacity(count as usize);
        for idx in 0..count {
            if (header & 1) == 0 {
                result.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(cursor.get_u32_be())), 2345));
            }
            else {
                // IPv6
            }
            header >> 1;
        }
        result
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

//impl<T: AsRef<[u8]>> From<T> for Message {
//    fn from(src: T) -> Self {
//        Message{ buffer: bytes::BytesMut::from(src.as_ref()) }
//    }
//}

impl From<&[u8; 64]> for  Message {
    fn from(src: &[u8; 64]) -> Self {
        Message{ buffer: bytes::BytesMut::from(&src[..]) }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Message {{ type: {:?}, epoch: {}, sequence_number: {}, members: {:?} }}",
               self.get_type(), self.get_epoch(), self.get_sequence_number(), self.get_members()
        )
    }
}
