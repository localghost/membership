use bytes::Buf;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use sha1::{Digest, Sha1};
use std::cmp::Ordering;
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{SystemTime, UNIX_EPOCH};

type Incarnation = u64;
type Id = [u8; 20];

#[derive(Debug)]
pub(crate) struct Member {
    pub(crate) id: Id,
    pub(crate) address: SocketAddr,
    pub(crate) incarnation: Incarnation,
}

impl Member {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Member {
            id: Member::generate_id(address),
            address,
            incarnation: 0,
        }
    }

    fn generate_id(address: SocketAddr) -> Id {
        let mut hasher = Sha1::new();
        match address {
            SocketAddr::V4(v4) => {
                hasher.input(v4.ip().octets());
                hasher.input(v4.port().to_be_bytes());
            }
            SocketAddr::V6(v6) => {
                hasher.input(v6.ip().octets());
                hasher.input(v6.port().to_be_bytes())
            }
        };
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => hasher.input(duration.as_millis().to_be_bytes()),
            Err(e) => hasher.input(SmallRng::from_entropy().gen::<u64>().to_be_bytes()),
        }
        hasher.result().into()
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
