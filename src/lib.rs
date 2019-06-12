use mio::*;
use mio::net::*;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr, Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::time::Duration;
use bytes::{BufMut, Buf, BytesMut};
use std::io::Cursor;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::fmt;
use log::{debug, info, error};
use std::default::Default;

mod message;
use crate::message::{MessageType, Message};

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

struct Ack {
    target: SocketAddr,
    epoch: u64,
    sequence_number: u64,
    request_time: std::time::Instant,
    originator: Option<SocketAddr>,
}

struct Confirmation {
    target: SocketAddr,
    epoch: u64,
    sequence_number: u64,
    indirect: Option<SocketAddr>
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
        let mut indirect_pings = Vec::with_capacity(1024);
        let mut indirect_acks = Vec::<Ack>::with_capacity(1024);
        let mut last_epoch_time = std::time::Instant::now();
        let mut acks = Vec::<Confirmation>::new();
        let mut new_epoch = false;
        loop {
            poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
            for event in events.iter() {
                debug!("{:?}", event);
                if event.readiness().is_readable() {
                    match event.token() {
                        Token(_) => {
                            let letter = self.recv_letter();
//                            if letter.message.get_epoch() != self.epoch {
//                                info!("Message not from this epoch: got={}, expected={}", letter.message.get_epoch(), self.epoch);
//                                continue;
//                            }

                            self.join_members(
                                letter.message.get_members().into_iter().chain(std::iter::once(letter.sender))
                            );
                            match letter.message.get_type() {
                                // FIXME: even when switching epochs it should not pause responding to Pings
                                message::MessageType::Ping => {
                                    acks.push(Confirmation{target: letter.sender, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number(), indirect: None});
                                    poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable()|Ready::writable(), PollOpt::edge()).unwrap();
                                }
                                message::MessageType::PingAck => {
                                    // check the key is in `direct_ack`, if it is not the node might have already be marked as failed
                                    // and removed from the cluster
                                    // 1. Check if the ack is in `indirect_acks`
                                    // 2. Check if it did not time out
                                    // 3. If it did time out, remove it from `indirect_acks` and `indirect_pings`
                                    // 4. If it did not time out, remove it from `indirect_acks` and move it to `indirect_confirms`
                                    // 5. Schedule sending `indirect_confirms`
                                    // 6. Make a single queue of ACKs to send and put it there
                                    for ack in indirect_acks.drain(..).collect::<Vec<Ack>>() {
                                        if letter.sender == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                            acks.push(Confirmation{target: ack.originator.unwrap(), epoch: ack.epoch, sequence_number: ack.sequence_number, indirect: Some(ack.target)})
                                        } else {
                                            indirect_acks.push(ack);
                                        }
                                    }
                                    if let Some(ref ack) = direct_ack {
                                        if letter.sender == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                            direct_ack = None;
                                        }
                                    } else if let Some(ref ack) = indirect_ack {
                                        if letter.sender == ack.target && letter.message.get_sequence_number() == ack.sequence_number {
                                            indirect_ack = None;
                                        }
                                    } else {
                                        debug!("Not waiting for this ACK, dropping it.");
                                    }
                                }
                                message::MessageType::PingIndirect => {
                                    indirect_pings.push(letter);
                                    poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::readable()|Ready::writable(), PollOpt::edge()).unwrap();
                                }
                            }
                        }
                        _ => unreachable!()
                    }
                } else if event.readiness().is_writable() {
                    match event.token() {
                        Token(43) => {
                            let pings = indirect_pings.drain(..).collect::<Vec<IncomingLetter>>();
                            for p in pings {
                                let mut message = Message::create(MessageType::Ping, p.message.get_sequence_number(), p.message.get_epoch());
                                self.send_letter(OutgoingLetter{ message, target: p.message.get_members()[0]});
                                indirect_acks.push(Ack { target: p.message.get_members()[0], epoch: p.message.get_epoch(), sequence_number: p.message.get_sequence_number(), request_time: std::time::Instant::now(), originator: Some(p.sender) });
                            }
                            if self.members.len() > 0 && new_epoch {
                                let target = self.members[self.next_member_index];
//                                self.send_ping(target, sequence_number);
                                let mut message = Message::create(MessageType::Ping, sequence_number, self.epoch);
                                // FIXME pick members with the lowest recently visited counter (mark to not starve the ones with highest visited counter)
                                // as that may lead to late failure discovery
                                message.with_members(
                                    &self.members.iter().skip(self.next_member_index).chain(self.members.iter().take(self.next_member_index)).cloned().collect::<Vec<_>>()
                                );
                                self.send_letter(OutgoingLetter { message, target });
                                direct_ack = Some(Ack { sequence_number, epoch: self.epoch, target, request_time: std::time::Instant::now(), originator: None });
                                sequence_number += 1;
                                self.next_member_index = (self.next_member_index + 1) % self.members.len();
                                new_epoch = false;
                            }
                            poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable()|Ready::writable(), PollOpt::edge()).unwrap();
                        }
                        Token(53) => {
                            // TODO: confirm pings until WouldBlock
                            if let Some(confirm) = acks.pop() {
                                let mut message = Message::create(MessageType::PingAck, confirm.sequence_number, confirm.epoch);
                                message.with_members(self.members.as_slice());
                                let letter = OutgoingLetter { message, target: confirm.target };
                                self.send_letter(letter);
                            } else {
                                poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable(), PollOpt::edge()).unwrap();
                            }
                        }
                        Token(63) => {
                            // TODO: PingIndirect
                            if let Some(ref mut ack) = indirect_ack {
                                for member in self.members.iter().take(3) {
                                    let mut message = Message::create(MessageType::PingIndirect, ack.sequence_number, self.epoch);
                                    message.with_members(&std::iter::once(&ack.target).chain(self.members.iter()).cloned().collect::<Vec<_>>());
                                    self.send_letter(OutgoingLetter { message, target: *member });
                                    ack.request_time = std::time::Instant::now();
                                }
                            }
                        }
                        _ => unreachable!()
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
                new_epoch = true;
                info!("New epoch: {}", self.epoch);
                poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable(), PollOpt::edge()).unwrap();
            }

            if let Some(ref ack) = direct_ack {
                if std::time::Instant::now() > (ack.request_time + Duration::from_secs(self.config.ack_timeout as u64)) {
                    indirect_ack = direct_ack;
                    direct_ack = None;
                    poll.reregister(self.server.as_ref().unwrap(), Token(63), Ready::writable(), PollOpt::edge()).unwrap();
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
        poll.register(self.server.as_ref().unwrap(), Token(43), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
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

