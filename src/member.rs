use bytes::Buf;
use std::cmp::Ordering;
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

type Incarnation = u64;

#[derive(Debug)]
pub(crate) struct Member {
    address: SocketAddr,
    incarnation: Incarnation,
}

impl Member {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Member {
            address,
            incarnation: 0,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    pub(crate) fn increment(&mut self) {
        self.incarnation += 1;
    }
}

impl PartialOrd for Member {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.address != other.address {
            return None;
        }
        self.incarnation.partial_cmp(&other.incarnation)
    }
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.incarnation == other.incarnation
    }
}

impl From<&[u8]> for Member {
    fn from(buffer: &[u8]) -> Self {
        let mut cursor = Cursor::new(buffer);
        match cursor.get_u8() {
            0 => Member {
                address: SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(cursor.get_u32_be()),
                    cursor.get_u16_be(),
                )),
                incarnation: cursor.get_u64_be(),
            },
            1 => unimplemented!(),
            _ => unreachable!(),
        }
    }
}
