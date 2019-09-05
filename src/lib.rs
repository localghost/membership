use log::{debug, info, warn};
use mio::net::*;
use mio::*;
use mio_extras::channel::{Receiver, Sender};
use std::collections::vec_deque::VecDeque;
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;

mod message;
mod unique_circular_buffer;
use crate::message::{Message, MessageType};
use crate::unique_circular_buffer::UniqueCircularBuffer;
use std::sync::{Arc, Mutex, RwLock};

#[derive(StructOpt)]
pub struct ProtocolConfig {
    #[structopt(short = "o", long = "proto-period", default_value = "5")]
    pub protocol_period: u64,

    #[structopt(short = "a", long = "ack-timeout", default_value = "1")]
    pub ack_timeout: u8,

    #[structopt(long = "num-indirect", default_value = "3")]
    pub num_indirect: u8,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            protocol_period: 5,
            ack_timeout: 1,
            num_indirect: 3,
        }
    }
}

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "j", long = "join-address", default_value = "127.0.0.1:2345")]
    pub join_address: SocketAddr,

    #[structopt(short = "b", long = "bind-address", default_value = "127.0.0.1:2345")]
    pub bind_address: SocketAddr,

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
    request_time: std::time::Instant,
}

impl Ack {
    fn new(request: Request) -> Self {
        Ack {
            request,
            request_time: std::time::Instant::now(),
        }
    }
}

#[derive(Debug)]
enum Request {
    Ping(Header),
    PingIndirect(Header),
    PingProxy(Header, SocketAddr),
    Ack(Header),
    AckIndirect(Header, SocketAddr),
}

#[derive(Debug)]
enum ChannelMessage {
    Stop,
    GetMembers(std::sync::mpsc::Sender<Vec<SocketAddr>>),
}

pub struct Gossip {
    config: ProtocolConfig,
    server: Option<UdpSocket>,
    members: Vec<SocketAddr>,
    dead_members: UniqueCircularBuffer<SocketAddr>,
    members_presence: HashSet<SocketAddr>,
    next_member_index: usize,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: [u8; 64],
    myself: SocketAddr,
    requests: VecDeque<Request>,
    receiver: Receiver<ChannelMessage>,
    acks: Vec<Ack>,
}

impl Gossip {
    fn new(bind_address: SocketAddr, config: ProtocolConfig) -> (Gossip, Sender<ChannelMessage>) {
        let (sender, receiver) = mio_extras::channel::channel();
        let gossip = Gossip {
            config,
            server: None,
            members: vec![],
            dead_members: UniqueCircularBuffer::new(5),
            members_presence: HashSet::new(),
            next_member_index: 0,
            epoch: 0,
            sequence_number: 0,
            recv_buffer: [0; 64],
            myself: bind_address,
            requests: VecDeque::<Request>::with_capacity(32),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
        };
        (gossip, sender)
    }

    fn join(&mut self, member: SocketAddr) {
        assert_ne!(member, self.myself, "Can't join yourself");

        self.update_members(std::iter::once(member), std::iter::empty());
        let poll = Poll::new().unwrap();
        poll.register(&self.receiver, Token(1), Ready::readable(), PollOpt::empty());
        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut last_epoch_time = std::time::Instant::now();

        let initial_ping = Request::Ping(Header {
            // the member we were given to join should be on the alive members list
            target: self.get_next_member().unwrap(),
            epoch: self.epoch,
            sequence_number: self.get_next_sequence_number(),
        });
        self.requests.push_front(initial_ping);

        'mainloop: loop {
            poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
            for event in events.iter() {
                match event.token() {
                    Token(0) => {
                        self.handle_protocol_event(&event);
                    }
                    Token(1) => match self.receiver.try_recv() {
                        Ok(message) => {
                            debug!("ChannelMessage::{:?}", message);
                            match message {
                                ChannelMessage::Stop => {
                                    break 'mainloop;
                                }
                                ChannelMessage::GetMembers(sender) => {
                                    sender.send(
                                        std::iter::once(&self.myself)
                                            .chain(self.members.iter())
                                            .cloned()
                                            .collect::<Vec<_>>(),
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            println!("Not ready yet");
                        }
                    },
                    _ => unreachable!(),
                }
            }

            let now = std::time::Instant::now();

            for ack in self.acks.drain(..).collect::<Vec<_>>() {
                if now > (ack.request_time + Duration::from_secs(self.config.ack_timeout as u64)) {
                    self.handle_timeout_ack(ack);
                } else {
                    self.acks.push(ack);
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
                sequence_number: self.get_next_sequence_number(),
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
            Request::PingIndirect(header) | Request::PingProxy(header, ..) => {
                // TODO: mark the member as suspected
                self.kill_members(std::iter::once(header.target));
            }
            _ => unreachable!(),
        }
    }

    fn bind(&mut self, poll: &Poll) {
        self.server = Some(UdpSocket::bind(&self.myself).unwrap());
        // FIXME: change to `PollOpt::edge()`
        poll.register(
            self.server.as_ref().unwrap(),
            Token(0),
            Ready::readable() | Ready::writable(),
            PollOpt::level(),
        )
        .unwrap();
    }

    fn send_letter(&self, letter: OutgoingLetter) {
        debug!("{:?}", letter);
        if let Err(e) = self.server.as_ref().unwrap().send_to(&letter.message.into_inner(), &letter.target) {
            warn!("Letter to {:?} was not delivered due to {:?}", letter.target, e);
        }
    }

    fn recv_letter(&mut self) -> Option<IncomingLetter> {
        match self.server.as_ref().unwrap().recv_from(&mut self.recv_buffer) {
            Ok((count, sender)) => {
                let letter = IncomingLetter {
                    sender,
                    message: message::Message::from_bytes(&self.recv_buffer, count),
                };
                debug!("{:?}", letter);
                Some(letter)
            }
            Err(e) => {
                warn!("Failed to receive letter due to {:?}", e);
                None
            }
        }
    }

    fn update_members<T1, T2>(&mut self, alive: T1, dead: T2)
    where
        T1: Iterator<Item = SocketAddr>,
        T2: Iterator<Item = SocketAddr>,
    {
        // 'alive' notification beats 'dead' notification
        self.remove_members(dead);
        for member in alive {
            if member == self.myself {
                continue;
            }
            if self.members_presence.insert(member) {
                info!("Member joined: {:?}", member);
                self.members.push(member);
            }
            if self.dead_members.remove(&member) > 0 {
                info!("Member {} found on the dead list", member);
            }
        }
    }

    fn kill_members<T>(&mut self, members: T)
    where
        T: Iterator<Item = SocketAddr>,
    {
        for member in members {
            self.remove_member(&member);
            self.dead_members.push(member);
        }
    }

    fn remove_members<T>(&mut self, members: T)
    where
        T: Iterator<Item = SocketAddr>,
    {
        for member in members {
            self.remove_member(&member);
        }
    }

    fn remove_member(&mut self, member: &SocketAddr) {
        if self.members_presence.remove(&member) {
            let idx = self.members.iter().position(|e| e == member).unwrap();
            self.members.remove(idx);
            if idx <= self.next_member_index && self.next_member_index > 0 {
                self.next_member_index -= 1;
            }
            info!("Member removed: {:?}", member);
        }
    }

    fn get_next_member(&mut self) -> Option<SocketAddr> {
        if self.members.is_empty() {
            return None;
        }

        let target = self.members[self.next_member_index];
        self.next_member_index = (self.next_member_index + 1) % self.members.len();
        Some(target)
    }

    fn get_next_sequence_number(&mut self) -> u64 {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        sequence_number
    }

    fn handle_protocol_event(&mut self, event: &Event) {
        if event.readiness().is_readable() {
            if let Some(letter) = self.recv_letter() {
                self.update_members_from_letter(&letter);
                match letter.message.get_type() {
                    message::MessageType::Ping => self.handle_ping(&letter),
                    message::MessageType::PingAck => self.handle_ack(&letter),
                    message::MessageType::PingIndirect => self.handle_indirect_ping(&letter),
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
                            &self
                                .members
                                .iter()
                                .filter(|&member| *member != header.target)
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_letter(OutgoingLetter {
                            message,
                            target: header.target,
                        });
                        self.acks.push(Ack::new(request));
                    }
                    Request::PingIndirect(ref header) => {
                        // FIXME do not send the message to the member that is being suspected
                        for member in self.members.iter().take(self.config.num_indirect as usize) {
                            let mut message = Message::create(MessageType::PingIndirect, header.sequence_number, header.epoch);
                            // filter is needed to not include target node on the alive list as it is being suspected
                            message.with_members(
                                &std::iter::once(&header.target)
                                    .chain(self.members.iter().filter(|&m| *m != header.target))
                                    .cloned()
                                    .collect::<Vec<_>>(),
                                &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                            );
                            self.send_letter(OutgoingLetter { message, target: *member });
                        }
                        self.acks.push(Ack::new(request));
                    }
                    Request::PingProxy(ref header, ..) => {
                        let mut message = Message::create(MessageType::Ping, header.sequence_number, header.epoch);
                        message.with_members(
                            &self
                                .members
                                .iter()
                                .filter(|&member| *member != header.target)
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_letter(OutgoingLetter {
                            message,
                            target: header.target,
                        });
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ack(header) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                        message.with_members(&self.members, &self.dead_members.iter().cloned().collect::<Vec<_>>());
                        self.send_letter(OutgoingLetter {
                            message,
                            target: header.target,
                        });
                    }
                    Request::AckIndirect(header, member) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                        message.with_members(
                            &std::iter::once(&member).chain(self.members.iter()).cloned().collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_letter(OutgoingLetter {
                            message,
                            target: header.target,
                        });
                    }
                }
            }
        }
    }

    fn update_members_from_letter(&mut self, letter: &IncomingLetter) {
        match letter.message.get_type() {
            message::MessageType::PingIndirect => self.update_members(
                letter
                    .message
                    .get_alive_members()
                    .into_iter()
                    .skip(1)
                    .chain(std::iter::once(letter.sender)),
                letter.message.get_dead_members().into_iter(),
            ),
            _ => self.update_members(
                letter.message.get_alive_members().into_iter().chain(std::iter::once(letter.sender)),
                letter.message.get_dead_members().into_iter(),
            ),
        }
    }

    fn handle_ack(&mut self, letter: &IncomingLetter) {
        for ack in self.acks.drain(..).collect::<Vec<_>>() {
            match ack.request {
                Request::PingIndirect(ref header) => {
                    if letter.message.get_alive_members()[0] == header.target
                        && letter.message.get_sequence_number() == header.sequence_number
                    {
                        continue;
                    }
                }
                Request::PingProxy(ref header, ref reply_to) => {
                    if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number {
                        self.requests.push_back(Request::AckIndirect(
                            Header {
                                target: *reply_to,
                                epoch: letter.message.get_epoch(),
                                sequence_number: letter.message.get_sequence_number(),
                            },
                            letter.sender,
                        ));
                        continue;
                    }
                }
                Request::Ping(ref header) => {
                    if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number {
                        continue;
                    }
                }
                _ => unreachable!(),
            }
            self.acks.push(ack);
        }
    }

    fn handle_ping(&mut self, letter: &IncomingLetter) {
        self.requests.push_back(Request::Ack(Header {
            target: letter.sender,
            epoch: letter.message.get_epoch(),
            sequence_number: letter.message.get_sequence_number(),
        }));
    }

    fn handle_indirect_ping(&mut self, letter: &IncomingLetter) {
        self.requests.push_back(Request::PingProxy(
            Header {
                target: letter.message.get_alive_members()[0],
                sequence_number: letter.message.get_sequence_number(),
                epoch: letter.message.get_epoch(),
            },
            letter.sender,
        ));
    }
}

pub struct Membership {
    bind_address: SocketAddr,
    config: Option<ProtocolConfig>,
    sender: Option<Sender<ChannelMessage>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Membership {
    pub fn new(bind_address: SocketAddr, config: ProtocolConfig) -> Self {
        Membership {
            bind_address,
            config: Some(config),
            sender: None,
            handle: None,
        }
    }

    pub fn join(&mut self, member: SocketAddr) {
        assert_ne!(member, self.bind_address, "Can't join yourself");

        let (mut gossip, sender) = Gossip::new(self.bind_address, self.config.take().unwrap());
        self.sender = Some(sender);
        self.handle = Some(std::thread::spawn(move || {
            gossip.join(member);
        }));
    }

    pub fn stop(&mut self) {
        self.sender.as_ref().unwrap().send(ChannelMessage::Stop);
        self.wait();
    }

    pub fn get_members(&self) -> Vec<SocketAddr> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.sender.as_ref().unwrap().send(ChannelMessage::GetMembers(sender));
        receiver.recv().unwrap()
    }

    pub fn wait(&mut self) {
        self.handle.take().unwrap().join();
    }
}
