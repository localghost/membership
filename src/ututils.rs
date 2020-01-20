#![cfg(test)]

use crate::member::Member;
use std::net::SocketAddr;
use std::str::FromStr;

pub(crate) fn create_members(count: usize) -> Vec<Member> {
    (0..count).map(create_member).collect()
}

pub(crate) fn create_member(index: usize) -> Member {
    Member::new(SocketAddr::from_str(&format!("127.0.0.1:{}", 1234 + index)).unwrap())
}
