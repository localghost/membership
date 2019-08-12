use mio::*;
use mio::net::*;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr, Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::time::Duration;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashSet};
use std::fmt;
use log::{debug, info};

mod message;
mod unique_circular_buffer;
use crate::message::{MessageType, Message};
use crate::unique_circular_buffer::UniqueCircularBuffer;
use std::sync::{Arc, Mutex};

#[derive(StructOpt, Default, Clone)]
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

#[derive(Debug)]
struct Ack {
    request: Request,
    request_time: std::time::Instant
}

impl Ack {
    fn new(request: Request) -> Self {
        Ack { request, request_time: std::time::Instant::now() }
    }
}

#[derive(Debug)]
enum Request {
    Ping(Header),
    PingIndirect(Header),
    PingProxy(Header, SocketAddr),
    Ack(Header),
    AckIndirect(Header, SocketAddr)
}

pub struct Gossip {
    config: ProtocolConfig,
    server: Option<UdpSocket>,
    members: Arc<Mutex<Vec<SocketAddr>>>,
    dead_members: UniqueCircularBuffer<SocketAddr>,
    members_presence: HashSet<SocketAddr>,
    next_member_index: usize,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: [u8; 64],
    myself: SocketAddr,
    requests: VecDeque<Request>
}

impl Gossip {
    pub fn new(config: ProtocolConfig) -> Gossip {
        let myself = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from_str(&config.bind_address).unwrap(), config.port));
        Gossip{
            config,
            server: None,
            members: Arc::new(Mutex::new(vec!())),
            dead_members: UniqueCircularBuffer::new(5),
            members_presence: HashSet::new(),
            next_member_index: 0,
            epoch: 0,
            sequence_number: 0,
            recv_buffer: [0; 64],
            myself,
            requests: VecDeque::<Request>::with_capacity(32)
        }
    }

    pub fn join(&mut self, _member: IpAddr) -> Arc<Mutex<Vec<SocketAddr>>> {
        self.update_members(std::iter::once(SocketAddr::new(_member, self.config.port)), std::iter::empty());
        let poll = Poll::new().unwrap();
        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut last_epoch_time = std::time::Instant::now();
        let mut acks = Vec::<Ack>::with_capacity(32);

        let initial_ping = Request::Ping(Header{
            // the member we were given to join should be on the alive members list
            target: self.get_next_member().unwrap(),
            epoch: self.epoch,
            sequence_number: self.get_next_sequence_number()
        });
        self.requests.push_front(initial_ping);

        loop {
            poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
            for event in events.iter() {
                if event.readiness().is_readable() {
                    let letter = self.recv_letter();
                    match letter.message.get_type() {
                        message::MessageType::PingIndirect => {
                            self.update_members(
                                letter.message.get_alive_members().into_iter().skip(1).chain(std::iter::once(letter.sender)),
                                letter.message.get_dead_members().into_iter()
                            );
                        }
                        _ => {
                            self.update_members(
                                letter.message.get_alive_members().into_iter().chain(std::iter::once(letter.sender)),
                                letter.message.get_dead_members().into_iter()
                            );
                        }
                    }
                    match letter.message.get_type() {
                        message::MessageType::Ping => {
                            self.requests.push_back(Request::Ack(
                                Header {
                                    target: letter.sender, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                },
                            ));
                        }
                        message::MessageType::PingAck => {
                            for ack in acks.drain(..).collect::<Vec<_>>() {
                                match ack.request {
                                    Request::PingIndirect(ref header) => {
                                        if letter.message.get_alive_members()[0] == header.target && letter.message.get_sequence_number() == header.sequence_number {
                                            continue;
                                        }
                                    }
                                    Request::PingProxy(ref header, ref reply_to) => {
                                        if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number {
                                            self.requests.push_back(Request::AckIndirect(
                                                Header {
                                                    target: *reply_to, epoch: letter.message.get_epoch(), sequence_number: letter.message.get_sequence_number()
                                                },
                                                letter.sender
                                            ));
                                            continue;
                                        }
                                    }
                                    Request::Ping(ref header) => {
                                        if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number {
                                            continue;
                                        }
                                    }
                                    _ => unreachable!()
                                }
                                acks.push(ack);
                            }
                        }
                        message::MessageType::PingIndirect => {
                            self.requests.push_back(Request::PingProxy(
                                Header {
                                    target: letter.message.get_alive_members()[0],
                                    sequence_number: letter.message.get_sequence_number(),
                                    epoch: letter.message.get_epoch()
                                },
                                letter.sender
                            ))
                        }
                    }
                } else if event.readiness().is_writable() {
                    if let Some(request) = self.requests.pop_front() {
                        debug!("{:?}", request);
                        match request {
                            Request::Ping(ref header) => {
                                let mut message = Message::create(MessageType::Ping, header.sequence_number, header.epoch);
                                // FIXME pick members with the lowest recently visited counter (mark to not starve the ones with highest visited counter)
                                // as that may lead to late failure discovery
                                message.with_members(
                                    &self.members.lock().unwrap().iter().filter(|&member|{*member != header.target}).cloned().collect::<Vec<_>>(),
                                    &self.dead_members.iter().cloned().collect::<Vec<_>>()
                                );
                                self.send_letter(OutgoingLetter { message, target: header.target });
                                acks.push(Ack::new(request));
                            }
                            Request::PingIndirect(ref header) => {
                                // FIXME do not send the message to the member that is being suspected
                                for member in self.members.lock().unwrap().iter().take(self.config.num_indirect as usize) {
                                    let mut message = Message::create(MessageType::PingIndirect, header.sequence_number, header.epoch);
                                    // filter is needed to not include target node on the alive list as it is being suspected
                                    message.with_members(
                                        &std::iter::once(&header.target).chain(self.members.lock().unwrap().iter().filter(|&m|{*m != header.target})).cloned().collect::<Vec<_>>(),
                                        &self.dead_members.iter().cloned().collect::<Vec<_>>()
                                    );
                                    self.send_letter(OutgoingLetter { message, target: *member });
                                }
                                acks.push(Ack::new(request));
                            },
                            Request::PingProxy(ref header, ..) => {
                                let mut message = Message::create(MessageType::Ping, header.sequence_number, header.epoch);
                                message.with_members(
                                    &self.members.lock().unwrap().iter().filter(|&member|{*member != header.target}).cloned().collect::<Vec<_>>(),
                                    &self.dead_members.iter().cloned().collect::<Vec<_>>()
                                );
                                self.send_letter(OutgoingLetter{ message, target: header.target });
                                acks.push(Ack::new(request));
                            }
                            Request::Ack(header) => {
                                let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                                message.with_members(&self.members.lock().unwrap(), &self.dead_members.iter().cloned().collect::<Vec<_>>());
                                self.send_letter(OutgoingLetter { message, target: header.target });
                            }
                            Request::AckIndirect(header, member) => {
                                let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                                message.with_members(
                                    &std::iter::once(&member).chain(self.members.lock().unwrap().iter()).cloned().collect::<Vec<_>>(),
                                    &self.dead_members.iter().cloned().collect::<Vec<_>>()
                                );
                                self.send_letter(OutgoingLetter { message, target: header.target });
                            }
                        }
                    }
                }
            }

            let now = std::time::Instant::now();

            for ack in acks.drain(..).collect::<Vec<_>>() {
                if now > (ack.request_time + Duration::from_secs(self.config.ack_timeout as u64)) {
                    self.handle_timeout_ack(ack);
                } else {
                    acks.push(ack);
                }
            }

            if now > (last_epoch_time + Duration::from_secs(self.config.protocol_period)) {
                self.advance_epoch();
                last_epoch_time = now;
            }
        }
    }

    fn advance_epoch(&mut self) {
        if let Some(member) = self.get_next_member() {
            let ping = Request::Ping(Header {
                target: member,
                epoch: self.epoch,
                sequence_number: self.get_next_sequence_number()
            });
            self.requests.push_front(ping);
        }
        self.epoch += 1;
        info!("New epoch: {}", self.epoch);
    }

    fn handle_timeout_ack(&mut self, ack: Ack) {
        match ack.request {
            Request::Ping(header) => {
                self.requests.push_back(Request::PingIndirect(header));
            }
            Request::PingIndirect(header)|Request::PingProxy(header, ..) => {
                // TODO: mark the member as suspected
                self.kill_members(std::iter::once(header.target));
            }
            _ => unreachable!()
        }
    }

    fn bind(&mut self, poll: &Poll) {
        let address = format!("{}:{}", self.config.bind_address, self.config.port).parse().unwrap();
        self.server = Some(UdpSocket::bind(&address).unwrap());
        // FIXME: change to `PollOpt::edge()`
        poll.register(self.server.as_ref().unwrap(), Token(0), Ready::readable() | Ready::writable(), PollOpt::level()).unwrap();
    }

    fn send_letter(&self, letter: OutgoingLetter) {
        debug!("{:?}", letter);
        self.server.as_ref().unwrap().send_to(&letter.message.into_inner(), &letter.target).unwrap();
    }

    fn recv_letter(&mut self) -> IncomingLetter {
        let (count, sender) = self.server.as_ref().unwrap().recv_from(&mut self.recv_buffer).unwrap();
        let letter = IncomingLetter{sender, message: message::Message::from_bytes(&self.recv_buffer, count)};
        debug!("{:?}", letter);
        letter
    }

    fn update_members<T1, T2>(&mut self, alive: T1, dead: T2)
        where T1: Iterator<Item=SocketAddr>, T2: Iterator<Item=SocketAddr>
    {
        // 'alive' notification beats 'dead' notification
        self.remove_members(dead);
        for member in alive {
            if member == self.myself {
                continue;
            }
            if self.members_presence.insert(member) {
                info!("Member joined: {:?}", member);
                self.members.lock().unwrap().push(member);
            }
            if self.dead_members.remove(&member) > 0 {
                info!("Member {} found on the dead list", member);
            }
        }
    }

    fn kill_members<T>(&mut self, members: T) where T: Iterator<Item = SocketAddr> {
        for member in members {
            self.remove_member(&member);
            self.dead_members.push(member);
        }
    }

    fn remove_members<T>(&mut self, members: T) where T: Iterator<Item = SocketAddr> {
        for member in members {
            self.remove_member(&member);
        }
    }

    fn remove_member(&mut self, member: &SocketAddr) {
        if self.members_presence.remove(&member) {
            let idx = self.members.lock().unwrap().iter().position(|e| { e == member }).unwrap();
            self.members.lock().unwrap().remove(idx);
            if idx <= self.next_member_index && self.next_member_index > 0 {
                self.next_member_index -= 1;
            }
            info!("Member removed: {:?}", member);
        }
    }

    fn get_next_member(&mut self) -> Option<SocketAddr> {
        if self.members.lock().unwrap().is_empty() {
            return None
        }

        let target = self.members.lock().unwrap()[self.next_member_index];
        self.next_member_index = (self.next_member_index + 1) % self.members.lock().unwrap().len();
        Some(target)
    }

    fn get_next_sequence_number(&mut self) -> u64 {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        sequence_number
    }
}

pub struct Gossip2 {
    config: ProtocolConfig,
    members: Option<Arc<Mutex<Vec<SocketAddr>>>>,
}

impl Gossip2 {
    pub fn new(config: ProtocolConfig) -> Self {
        Gossip2 {
            config,
            members: None,
        }
    }

    pub fn join(&mut self, member: IpAddr) {
        let mut gossip = Gossip::new(self.config.clone());
        self.members = Some(gossip.members.clone());
        std::thread::spawn(move || {
           gossip.join(member);
        });
    }

    pub fn get_members(&self) -> Vec<SocketAddr> {
        println!("Trying to get members");
        self.members.as_ref().unwrap().clone().lock().unwrap().clone()
    }
}