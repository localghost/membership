#![deny(missing_docs)]

//! Implementation of SWIM protocol.
//!
//! Please refer to [SWIM paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) for detailed description.
//!
//! # Examples
//! ```
//! use membership::{Membership, ProtocolConfig};
//! use std::net::SocketAddr;
//! use failure::_core::str::FromStr;
//! use failure::_core::time::Duration;
//!
//! let mut ms1 = Membership::new(SocketAddr::from_str("127.0.0.1:2345").unwrap(), Default::default());
//! let mut ms2 = Membership::new(SocketAddr::from_str("127.0.0.1:3456").unwrap(), Default::default());
//! ms1.join(SocketAddr::from_str("127.0.0.1:3456").unwrap()).unwrap();
//! ms2.join(SocketAddr::from_str("127.0.0.1:2345").unwrap()).unwrap();
//! std::thread::sleep(Duration::from_secs(ProtocolConfig::default().protocol_period * 2));
//! println!("{:?}", ms1.get_members().unwrap());
//! println!("{:?}", ms2.get_members().unwrap());
//! ms1.stop().unwrap();
//! ms2.stop().unwrap();
//! ```

use failure::{format_err, Error, ResultExt};
use mio::net::*;
use mio::*;
use mio_extras::channel::{Receiver, Sender};
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use structopt::StructOpt;
mod message;

mod unique_circular_buffer;
use crate::message::{Message, MessageType};
use crate::unique_circular_buffer::UniqueCircularBuffer;
use log::{debug, info, warn};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

type Result<T> = std::result::Result<T, Error>;

#[derive(StructOpt)]
/// Configuration for the membership protocol.
pub struct ProtocolConfig {
    /// Number of seconds between checking new member.
    #[structopt(short = "o", long = "proto-period", default_value = "5")]
    pub protocol_period: u64,

    /// Number of seconds to wait for response from a peer.
    ///
    /// Must be significantly (e.g. four times) smaller than `protocol_period`.
    #[structopt(short = "a", long = "ack-timeout", default_value = "1")]
    pub ack_timeout: u8,

    /// Maximum number of members selected for indirect probing.
    ///
    /// When probed member does not respond in `ack_timeout`, `num_indirect` other members are asked to check
    /// the probed member.
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

struct IncomingLetter {
    sender: SocketAddr,
    message: message::Message,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingLetter {{ sender: {}, message: {:?} }}",
            self.sender, self.message
        )
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

struct Member {
    address: SocketAddr,
    counter: u64, // dissemination counter
}

struct DisseminatedMembers {
    members: HashMap<SocketAddr, u64>,
}

impl DisseminatedMembers {
    fn get_least_disseminated(&self) {}
}

struct Gossip {
    config: ProtocolConfig,
    server: Option<UdpSocket>,
    members: Vec<SocketAddr>,
    dead_members: UniqueCircularBuffer<SocketAddr>,
    members_presence: HashSet<SocketAddr>,
    members_details: HashMap<SocketAddr, Member>,
    next_member_index: usize,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: [u8; 64],
    myself: SocketAddr,
    requests: VecDeque<Request>,
    receiver: Receiver<ChannelMessage>,
    acks: Vec<Ack>,
    rng: SmallRng,
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
            members_details: HashMap::new(),
            next_member_index: 0,
            epoch: 0,
            sequence_number: 0,
            recv_buffer: [0; 64],
            myself: bind_address,
            requests: VecDeque::<Request>::with_capacity(32),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
            rng: SmallRng::from_entropy(),
        };
        (gossip, sender)
    }

    fn join(&mut self, member: SocketAddr) -> Result<()> {
        assert_ne!(member, self.myself, "Can't join yourself");

        self.update_members(std::iter::once(member), std::iter::empty());
        let poll = Poll::new().unwrap();
        poll.register(&self.receiver, Token(1), Ready::readable(), PollOpt::empty())?;
        self.bind(&poll)?;

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
                                    let members = std::iter::once(&self.myself)
                                        .chain(self.members.iter())
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if let Err(e) = sender.send(members) {
                                        warn!("Failed to send list of members: {:?}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Not ready yet: {:?}", e);
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
                self.show_metrics();
                self.advance_epoch();
                last_epoch_time = now;
            }
        }

        Ok(())
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

    fn bind(&mut self, poll: &Poll) -> Result<()> {
        self.server = Some(UdpSocket::bind(&self.myself).context("Failed to bind to socket")?);
        // FIXME: change to `PollOpt::edge()`
        poll.register(
            self.server.as_ref().unwrap(),
            Token(0),
            Ready::readable() | Ready::writable(),
            PollOpt::level(),
        )
        .map_err(|e| format_err!("Failed to register socket for polling: {:?}", e))
    }

    fn send_message(&mut self, target: &SocketAddr, message: &Message) {
        debug!("{:?} <- {:?}", target, message);
        let result = self.server.as_ref().unwrap().send_to(&message.buffer(), target);
        if let Err(e) = result {
            warn!("Message to {:?} was not delivered due to {:?}", target, e);
        } else {
            self.update_member_counters(&message.get_alive_members());
        }
    }

    fn update_member_counters(&mut self, members: &[SocketAddr]) {
        members
            .iter()
            .for_each(|m| self.members_details.get_mut(m).unwrap().counter += 1);
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
                self.members_details.insert(
                    member,
                    Member {
                        address: member,
                        counter: 0,
                    },
                );
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
            self.members_details.remove(&member);
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
        // Following SWIM paper, section 4.3, next member to probe is picked in round-robin fashion, with all
        // the members randomly shuffled after each one has been probed.
        // FIXME: one thing that is missing is that new members are always added at the end instead of at uniformly
        // random position.
        if self.next_member_index == 0 {
            self.members.shuffle(&mut self.rng);
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
                                .get_least_disseminated_members()
                                .iter()
                                .filter(|&member| *member != header.target)
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::PingIndirect(ref header) => {
                        let indirect_members = self
                            .get_least_disseminated_members()
                            .iter()
                            // do not send the message to the member that is being suspected
                            .filter(|&m| *m != header.target)
                            .take(self.config.num_indirect as usize)
                            .cloned()
                            .collect::<Vec<_>>();
                        for member in indirect_members {
                            let mut message =
                                Message::create(MessageType::PingIndirect, header.sequence_number, header.epoch);
                            // filter is needed to not include target node on the alive list as it is being suspected
                            message.with_members(
                                &std::iter::once(&header.target)
                                    .chain(
                                        self.get_least_disseminated_members()
                                            .iter()
                                            .filter(|&m| *m != header.target),
                                    )
                                    .cloned()
                                    .collect::<Vec<_>>(),
                                &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                            );
                            self.send_message(&member, &message);
                        }
                        self.acks.push(Ack::new(request));
                    }
                    Request::PingProxy(ref header, ..) => {
                        let mut message = Message::create(MessageType::Ping, header.sequence_number, header.epoch);
                        message.with_members(
                            &self
                                .get_least_disseminated_members()
                                .iter()
                                .filter(|&member| *member != header.target)
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ack(header) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                        message.with_members(
                            &self.get_least_disseminated_members(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
                    }
                    Request::AckIndirect(header, member) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number, header.epoch);
                        message.with_members(
                            &std::iter::once(&member)
                                .chain(self.get_least_disseminated_members().iter())
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
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
                letter
                    .message
                    .get_alive_members()
                    .into_iter()
                    .chain(std::iter::once(letter.sender)),
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
                    if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number
                    {
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
                    if letter.sender == header.target && letter.message.get_sequence_number() == header.sequence_number
                    {
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

    fn get_least_disseminated_members(&self) -> Vec<SocketAddr> {
        let mut members = self.members_details.values().collect::<Vec<_>>();
        members.sort_by(|a, b| a.counter.cmp(&b.counter));
        members.iter().map(|m| m.address).collect::<Vec<_>>()
    }

    fn show_metrics(&self) {
        for member in self.members_details.values() {
            debug!("{:?} -> {:?}: {}", self.myself, member.address, member.counter);
        }
    }
}

/// Runs the gossip protocol on an internal thread.
pub struct Membership {
    bind_address: SocketAddr,
    config: Option<ProtocolConfig>,
    sender: Option<Sender<ChannelMessage>>,
    handle: Option<std::thread::JoinHandle<Result<()>>>,
}

impl Membership {
    /// Creates new instance communicating with other members through `bind_address`.
    pub fn new(bind_address: SocketAddr, config: ProtocolConfig) -> Self {
        Membership {
            bind_address,
            config: Some(config),
            sender: None,
            handle: None,
        }
    }

    /// Returns bind address of this member.
    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    /// Joins the group through `member` which has to already belong to the group.
    ///
    /// Member might not be instantly spotted by all other members of the group.
    pub fn join(&mut self, member: SocketAddr) -> Result<()> {
        assert_ne!(member, self.bind_address, "Can't join yourself");
        assert!(self.handle.is_none(), "You have already joined");

        let (mut gossip, sender) = Gossip::new(self.bind_address, self.config.take().unwrap());
        self.sender = Some(sender);
        self.handle = Some(
            std::thread::Builder::new()
                .name("membership".to_string())
                .spawn(move || gossip.join(member))?,
        );
        Ok(())
    }

    /// Stops this member, removing it from the group.
    ///
    /// Member does not broadcast that its quiting (at least not yet), thus it may still be observed by others
    /// as alive, at least for a short period of time.
    pub fn stop(&mut self) -> Result<()> {
        assert!(self.handle.is_some(), "You have not joined yet");

        self.sender
            .as_ref()
            .unwrap()
            .send(ChannelMessage::Stop)
            .map_err(|e| format_err!("Failed to stop message: {:?}", e))?;
        self.wait()
    }

    /// Returns all alive members of the group this member knows about.
    ///
    /// These might not necessary be all alive members in the entire group.
    pub fn get_members(&self) -> Result<Vec<SocketAddr>> {
        assert!(self.handle.is_some(), "First you have to join");

        let (sender, receiver) = std::sync::mpsc::channel();
        self.sender
            .as_ref()
            .unwrap()
            .send(ChannelMessage::GetMembers(sender))
            .map_err(|e| format_err!("Failed to ask for members: {:?}", e))?;
        receiver
            .recv()
            .map_err(|e| format_err!("Failed to get members: {:?}", e))
    }

    #[doc(hidden)]
    /// Waits for the member to finish.
    pub fn wait(&mut self) -> Result<()> {
        assert!(self.handle.is_some(), "You have not joined yet");
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|e| format_err!("Membership thread panicked: {:?}", e))?
    }
}
