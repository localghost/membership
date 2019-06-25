use mio::*;
use mio::net::*;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr, Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::time::Duration;
use bytes::{BufMut, Buf, BytesMut};
use std::io::Cursor;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet, BTreeMap, BinaryHeap};
use std::fmt;
use log::{debug, info, error};
use std::default::Default;

mod message;
use crate::message::{MessageType, Message};
use std::cmp::Reverse;
use std::convert::TryInto;

#[derive(StructOpt, Default)]
pub struct ProtocolConfig {
    #[structopt(short="p", long="port", default_value="2345")]
    port: u16,

    #[structopt(short="b", long="bind-address", default_value="127.0.0.1")]
    bind_address: String,

    #[structopt(short="o", long="proto-period", default_value="5")]
    protocol_period: u64,

    #[structopt(short="a", long="ack-timeout", default_value="1")]
    ack_timeout: u8,

    #[structopt(long="num-indirect", default_value="3")]
    num_indirect: u8,
}

#[derive(StructOpt, Default)]
pub struct Config {
    #[structopt(short="j", long="join-address", default_value="127.0.0.1")]
    pub join_address: String,

    // FIXME: this should not be public, fix dependencies between the two configs, make clear which is about protocol
    // and which about client properties.
    #[structopt(flatten)]
    pub proto_config: ProtocolConfig,
}

struct OutgoingLetter {
    target: SocketAddr,
    message: message::Message,
}

impl fmt::Debug for OutgoingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutgoingLetter {{ target: {}, message: {:?} }}", self.target, self.message)
    }
}

struct IncomingLetter {
    sender: SocketAddr,
    message: message::Message,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IncomingLetter {{ sender: {}, message: {:?} }}", self.sender, self.message)
    }
}

#[derive(Debug)]
struct Header {
    target: SocketAddr,
    epoch: u64,
    sequence_number: u64,
}

struct Ack {
    target: SocketAddr,
    epoch: u64,
    sequence_number: u64,
    request_time: std::time::Instant,
    indirect: bool,
    originator: Option<SocketAddr>,
}

struct Ack2 {
    request: Request,
    request_time: std::time::Instant
}

impl Ord for Ack {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.request_time.cmp(&other.request_time)
    }
}

impl PartialOrd for Ack {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.request_time.partial_cmp(&other.request_time)
    }
}

impl Eq for Ack { }

impl PartialEq for Ack {
    fn eq(&self, other: &Self) -> bool {
        self.request_time.eq(&other.request_time)
    }
}

#[derive(Debug)]
enum Request {
    Ping,
    PingIndirect(Header),
    PingProxy(Header, SocketAddr),
    Ack(Header),
    AckIndirect(Header, SocketAddr)
}

pub struct Gossip {
    config: ProtocolConfig,
    server: Option<UdpSocket>,
    members: Vec<SocketAddr>,
    members_presence: HashSet<SocketAddr>,
    next_member_index: usize,
    epoch: u64,
    recv_buffer: [u8; 64],
    myself: SocketAddr,
}

impl Gossip {
    pub fn new(config: ProtocolConfig) -> Gossip {
        let myself = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from_str(&config.bind_address).unwrap(), config.port));
        Gossip{
            config,
            server: None,
            members: vec!(),
            members_presence: HashSet::new(),
            next_member_index: 0,
            epoch: 0,
            recv_buffer: [0; 64],
            myself
        }
    }

    pub fn join(&mut self, _member: IpAddr) {
        self.join_members(std::iter::once(SocketAddr::new(_member, self.config.port)));
        let poll = Poll::new().unwrap();
        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut sequence_number: u64 = 0;
        let mut last_epoch_time = std::time::Instant::now();
        let mut new_epoch = false;
        let mut requests = VecDeque::<Request>::new();
        let mut acks = Vec::<Ack>::new();

        requests.push_back(Request::Ping);
        loop {
            poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
            for event in events.iter() {
//                debug!("{:?}", event);
                if event.readiness().is_readable() {
                    let letter = self.recv_letter();
//                            if letter.message.get_epoch() != self.epoch {
//                                info!("Message not from this epoch: got={}, expected={}", letter.message.get_epoch(), self.epoch);
//                                continue;
//                            }
                    self.join_members(
                        letter.message.get_members().into_iter().chain(std::iter::once(letter.sender))
                    );
                    match letter.message.get_type() {
                        message::MessageType::Ping => {
                            requests.push_back(Request::Ack(
                                Header {
                                    target: letter.sender, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                },
                            ));
                        }
                        message::MessageType::PingAck => {
                            for ack in acks.drain(..).collect::<Vec<Ack>>() {
                                if ack.indirect {
                                    if letter.message.get_members()[0] == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                        continue;
                                    }
                                } else {
                                    if letter.sender == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                        if let Some(reply_to) = ack.originator {
                                            requests.push_back(Request::AckIndirect(
                                               Header {
                                                   target: reply_to, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                               },
                                               letter.sender
                                            ));
                                        }
                                        continue;
                                    }
                                }
                                acks.push(ack);
                            }
                        }
                        message::MessageType::PingIndirect => {
                            requests.push_back(Request::PingProxy(
                                Header {
                                    target: letter.message.get_members()[0],
                                    sequence_number: letter.message.get_sequence_number(),
                                    epoch: letter.message.get_epoch()
                                },
                                letter.sender
                            ))
                        }
                    }
                } else if event.readiness().is_writable() {
                    if let Some(request) = requests.pop_front() {
                        debug!("{:?}", request);
                        match request {
                            Request::Ping => {
                                if self.members.len() > 0 {
                                    let target = self.members[self.next_member_index];
//                                self.send_ping(target, sequence_number);
                                    let mut message = Message::create(MessageType::Ping, sequence_number, self.epoch);
                                    // FIXME pick members with the lowest recently visited counter (mark to not starve the ones with highest visited counter)
                                    // as that may lead to late failure discovery
                                    message.with_members(
                                        &self.members.iter().skip(self.next_member_index).chain(self.members.iter().take(self.next_member_index)).cloned().collect::<Vec<_>>()
                                    );
                                    self.send_letter(OutgoingLetter { message, target });
//                                            ack = Some(Ack { sequence_number, epoch: self.epoch, target, request_time: std::time::Instant::now(), originator: None });
//                                            new_acks.push(Reverse(Ack { sequence_number, epoch: self.epoch, target, request_time: std::time::Instant::now(), originator: None }));
                                    acks.push(Ack { sequence_number, epoch: self.epoch, target, request_time: std::time::Instant::now(), indirect: false, originator: None });
                                    sequence_number += 1;
                                    self.next_member_index = (self.next_member_index + 1) % self.members.len();
                                }
                            }
                            Request::PingIndirect(header) => {
                                for member in self.members.iter().take(self.config.num_indirect as usize) {
                                    let mut message = Message::create(MessageType::PingIndirect, header.sequence_number, header.epoch);
                                    message.with_members(&std::iter::once(&header.target).chain(self.members.iter()).cloned().collect::<Vec<_>>());
                                    self.send_letter(OutgoingLetter { message, target: *member });
                                }
                                acks.push(Ack{
                                    target: header.target,
                                    epoch: header.epoch,
                                    sequence_number: header.sequence_number,
                                    request_time: std::time::Instant::now(),
                                    indirect: true,
                                    originator: None
                                });
                            },
                            Request::PingProxy(header, reply_to) => {
                                let mut message = Message::create(MessageType::Ping, header.sequence_number, header.epoch);
//                                        message.with_members()
                                self.send_letter(OutgoingLetter{ message, target: header.target });
                                acks.push(Ack { sequence_number: header.sequence_number, epoch: header.epoch, target: header.target, request_time: std::time::Instant::now(), indirect: false, originator: Some(reply_to) });
                            }
                            Request::Ack(header) => {
                                let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                                message.with_members(&self.members);
                                self.send_letter(OutgoingLetter { message, target: header.target });
                            }
                            Request::AckIndirect(header, member) => {
                                let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                                message.with_members(&std::iter::once(&member).chain(self.members.iter()).cloned().collect::<Vec<_>>());
                                self.send_letter(OutgoingLetter { message, target: header.target });
                            }
                        }
                    }
                }
            }

            let now = std::time::Instant::now();

            for ack in acks.drain(..).collect::<Vec<Ack>>() {
                if std::time::Instant::now() > (ack.request_time + Duration::from_secs(self.config.ack_timeout as u64)) {
                    if !ack.indirect {
                        requests.push_back(Request::PingIndirect(
                            Header { target: ack.target, epoch: ack.epoch, sequence_number: ack.sequence_number }
                        ));
                    } else {
                        self.remove_members(std::iter::once(ack.target));
                        // TODO: mark the member as suspected
                    }
                }
                else {
                    acks.push(ack);
                }
            }

            if now > (last_epoch_time + Duration::from_secs(self.config.protocol_period)) {
                self.epoch += 1;
                info!("New epoch: {}", self.epoch);
                last_epoch_time = now;
                requests.push_front(Request::Ping);
            }
        }
    }

    fn bind(&mut self, poll: &Poll) {
        let address = format!("{}:{}", self.config.bind_address, self.config.port).parse().unwrap();
        self.server = Some(UdpSocket::bind(&address).unwrap());
        poll.register(self.server.as_ref().unwrap(), Token(0), Ready::readable() | Ready::writable(), PollOpt::level()).unwrap();
    }

    fn send_letter(&self, letter: OutgoingLetter) {
        debug!("{:?}", letter);
        self.server.as_ref().unwrap().send_to(&letter.message.into_inner(), &letter.target).unwrap();
    }

    fn recv_letter(&mut self) -> IncomingLetter {
        let (_, sender) = self.server.as_ref().unwrap().recv_from(&mut self.recv_buffer).unwrap();
        let letter = IncomingLetter{sender, message: message::Message::from(&self.recv_buffer)};
        debug!("{:?}", letter);
        letter
    }

    fn join_members<T>(&mut self, members: T) where T: Iterator<Item = SocketAddr> {
        for member in members {
            if member == self.myself {
                continue;
            }
            if self.members_presence.insert(member) {
                info!("Member joined: {:?}", member);
                self.members.push(member);
            }
        }
    }

    fn remove_members<T>(&mut self, members: T) where T: Iterator<Item = SocketAddr> {
        for member in members {
            if self.members_presence.remove(&member) {
                let idx = self.members.iter().position(|e| { *e == member }).unwrap();
                self.members.remove(idx);
                if idx <= self.next_member_index && self.next_member_index > 0 {
                    self.next_member_index -= 1;
                }
                info!("Member removed: {:?}", member);
            }
        }
    }
}

