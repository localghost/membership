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
struct Triple {
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

struct Confirmation {
    triple: Triple,
    indirect: Option<SocketAddr>
}

struct PingRequest {
    target: SocketAddr,
    epoch: u64,
    sequence_number: u64,
    reply_to: Option<SocketAddr>
}

struct IndirectPingRequest {
    triple: Triple,
    reply_to: SocketAddr
}

impl Into<IndirectAck> for IndirectPingRequest {
    fn into(self) -> IndirectAck {
        IndirectAck {
            triple: self.triple,
            reply_to: self.reply_to
        }
    }
}

struct IndirectAck {
    triple: Triple,
    reply_to: SocketAddr
}

impl Into<Confirmation> for IndirectAck {
    fn into(self) -> Confirmation {
        Confirmation{
            triple: Triple {
                target: self.reply_to,
                epoch: self.triple.epoch,
                sequence_number: self.triple.sequence_number,
            },
            indirect: Some(self.triple.target)
        }
    }
}

#[derive(Debug)]
enum Request {
    Ping,
    IndirectPingRequest  {
        triple: Triple,
    },
    PingForward {
        triple: Triple,
        reply_to: SocketAddr,
    },
    Confirmation {
        triple: Triple,
        indirect: Option<SocketAddr>
    }
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
        let mut direct_ack: Option<Ack> = None;
        let mut indirect_ack: Option<Ack> = None;
        let mut indirect_pings = Vec::<IndirectPingRequest>::with_capacity(1024);
        let mut indirect_acks = Vec::<Ack>::with_capacity(1024);
        let mut last_epoch_time = std::time::Instant::now();
        let mut acks = Vec::<Confirmation>::new();
        let mut ack: Option<Ack> = None;
        let mut new_epoch = false;
        let mut ping_requests = VecDeque::<PingRequest>::new();
        let mut requests = VecDeque::<Request>::new();
        let mut new_acks = BinaryHeap::<Reverse<Ack>>::new();
        let mut acks2 = Vec::<Ack>::new();

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
                            requests.push_back(Request::Confirmation {
                                triple: Triple{
                                    target: letter.sender, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                },
                                indirect: None
                            });
                        }
                        message::MessageType::PingAck => {
                            for ack in acks2.drain(..).collect::<Vec<Ack>>() {
                                if ack.indirect {
                                    if letter.message.get_members()[0] == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                        continue;
                                    }
                                } else {
                                    if letter.sender == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                        if let Some(reply_to) = ack.originator {
                                            requests.push_back(Request::Confirmation {
                                                triple: Triple{
                                                    target: reply_to, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                                },
                                                indirect: Some(letter.sender)
                                            });
                                        }
                                        continue;
                                    }
                                }
                                acks2.push(ack);
                            }
                        }
                        message::MessageType::PingIndirect => {
                            requests.push_back(Request::PingForward {
                                triple: Triple{
                                    target: letter.message.get_members()[0],
                                    sequence_number: letter.message.get_sequence_number(),
                                    epoch: letter.message.get_epoch()
                                },
                                reply_to: letter.sender
                            })
                        }
                    }
                } else if event.readiness().is_writable() {
                    match event.token() {
                        Token(_) => {
                            while let Some(request) = requests.pop_front() {
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
                                            acks2.push(Ack { sequence_number, epoch: self.epoch, target, request_time: std::time::Instant::now(), indirect: false, originator: None });
                                            sequence_number += 1;
                                            self.next_member_index = (self.next_member_index + 1) % self.members.len();
                                        }
                                    }
                                    Request::IndirectPingRequest{triple} => {
                                        for member in self.members.iter().take(self.config.num_indirect as usize) {
                                            let mut message = Message::create(MessageType::PingIndirect, triple.sequence_number, triple.epoch);
                                            message.with_members(&std::iter::once(&triple.target).chain(self.members.iter()).cloned().collect::<Vec<_>>());
                                            self.send_letter(OutgoingLetter { message, target: *member });
                                        }
                                        acks2.push(Ack{
                                            target: triple.target,
                                            epoch: triple.epoch,
                                            sequence_number: triple.sequence_number,
                                            request_time: std::time::Instant::now(),
                                            indirect: true,
                                            originator: None
                                        });
                                    },
                                    Request::PingForward{triple, reply_to} => {
                                        let mut message = Message::create(MessageType::Ping, triple.sequence_number, triple.epoch);
//                                        message.with_members()
                                        self.send_letter(OutgoingLetter{ message, target: triple.target });
                                        acks2.push(Ack { sequence_number: triple.sequence_number, epoch: triple.epoch, target: triple.target, request_time: std::time::Instant::now(), indirect: false, originator: Some(reply_to) });
                                    }
                                    Request::Confirmation{triple, indirect} => {
                                        let mut message = Message::create(MessageType::PingAck, triple.sequence_number, triple.epoch);
                                        if let Some(member) = indirect {
                                            message.with_members(&std::iter::once(&member).chain(self.members.iter()).cloned().collect::<Vec<_>>());
                                        } else {
                                            message.with_members(&self.members);
                                        }
                                        let letter = OutgoingLetter { message, target: triple.target };
                                        self.send_letter(letter);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let now = std::time::Instant::now();
            if now > (last_epoch_time + Duration::from_secs(self.config.protocol_period)) {
                self.epoch += 1;
                // TODO: mark the nodes as suspected first.
                if let Some(ref ack) = indirect_ack {
                    self.remove_members(std::iter::once(ack.target));
                }
                indirect_ack = None;
                last_epoch_time = now;
                requests.push_front(Request::Ping);
                info!("New epoch: {}", self.epoch);
//                if self.members.len() > 0 {
//                    ping_requests.push_front(PingRequest { target: self.members});
//                }
//                poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable(), PollOpt::edge()).unwrap();
            }

            for ack in acks2.drain(..).collect::<Vec<Ack>>() {
                if std::time::Instant::now() > (ack.request_time + Duration::from_secs(self.config.ack_timeout as u64)) {
                    if !ack.indirect {
                        requests.push_back(Request::IndirectPingRequest {
                            triple: Triple { target: ack.target, epoch: ack.epoch, sequence_number: ack.sequence_number }
                        });
                    } else {
                        self.remove_members(std::iter::once(ack.target));
                        // TODO: mark the member as failed
                    }
                }
                else {
                    acks2.push(ack);
                }
            }
//            let mut drained: BTreeMap<std::time::Instant, u64>;
//            let (drained, new_timeouts): (BTreeMap<std::time::Instant, u64>, BTreeMap<std::time::Instant, u64>) = ack_timeouts.into_iter().partition(|(t, _)| {now > (*t + Duration::from_secs(self.config.ack_timeout as u64))});
//            for (time, sequence_number) in drained {
//                if let Some((seqnum, target)) = direct_ack {
//                    // ping indirect
//                    poll.reregister(self.server.as_ref().unwrap(), Token(63), Ready::writable(), PollOpt::edge()).unwrap();
////                    self.remove_members(std::iter::once(removed));
//                }
//            }
//            ack_timeouts = new_timeouts;
//            self.check_ack();
        }
    }

//    fn send_ping(&mut self, target: SocketAddr, sequence_number: u64) {
//        let mut message = Message::create(MessageType::Ping, sequence_number, self.epoch);
//        // FIXME pick members with the lowest recently visited counter (mark to not starve the ones with highest visited counter)
//        // as that may lead to late failure discovery
//        let member_index = self.members.iter().position(|x| { *x == target}).unwrap();
//        message.with_members(
//            &self.members.iter().skip(member_index).chain(self.members.iter().take(member_index)).cloned().collect::<Vec<_>>()
//        );
//        self.send_letter(OutgoingLetter { message, target });
////        wait_ack.insert(sequence_number, target);
//    }

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

    fn increment_epoch(&mut self) {
        //
    }

    fn ping_indirect(&mut self) {

    }

    fn check_ack(&mut self) {

    }
}

