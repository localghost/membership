#![deny(missing_docs)]

use crate::incoming_message::{DisseminationMessage, IncomingMessage, PingRequestMessage};
use crate::least_disseminated_members::DisseminatedMembers;
use crate::message::{Message, MessageType};
use crate::message_decoder::decode_message;
use crate::result::Result;
use crate::suspicion::Suspicion;
use crate::unique_circular_buffer::UniqueCircularBuffer;
use crate::ProtocolConfig;
use failure::{format_err, ResultExt};
use log::{debug, info, warn};
use mio::net::UdpSocket;
use mio::{Event, Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel::{Receiver, Sender};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

struct IncomingLetter {
    sender: SocketAddr,
    message: IncomingMessage,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingLetter {{ sender: {:#?}, message: {:#?} }}",
            self.sender, self.message
        )
    }
}

#[derive(Debug)]
struct Header {
    target: SocketAddr,
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
pub(crate) enum ChannelMessage {
    Stop,
    GetMembers(std::sync::mpsc::Sender<Vec<SocketAddr>>),
}

/// Runs the protocol on current thread, blocking it.
pub(crate) struct SyncNode {
    config: ProtocolConfig,
    server: Option<UdpSocket>,
    members: Vec<SocketAddr>,
    alive_disseminated_members: DisseminatedMembers,
    suspected_disseminated_members: DisseminatedMembers,
    dead_members: UniqueCircularBuffer<SocketAddr>,
    members_presence: HashSet<SocketAddr>,
    next_member_index: usize,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: Vec<u8>,
    myself: SocketAddr,
    requests: VecDeque<Request>,
    receiver: Receiver<ChannelMessage>,
    acks: Vec<Ack>,
    rng: SmallRng,
    suspicions: VecDeque<Suspicion>,
}

impl SyncNode {
    pub(crate) fn new(bind_address: SocketAddr, config: ProtocolConfig) -> (SyncNode, Sender<ChannelMessage>) {
        let (sender, receiver) = mio_extras::channel::channel();
        let gossip = SyncNode {
            config,
            server: None,
            members: vec![],
            alive_disseminated_members: DisseminatedMembers::new(),
            suspected_disseminated_members: DisseminatedMembers::new(),
            dead_members: UniqueCircularBuffer::new(5),
            members_presence: HashSet::new(),
            next_member_index: 0,
            epoch: 0,
            sequence_number: 0,
            recv_buffer: Vec::with_capacity(1500),
            myself: bind_address,
            requests: VecDeque::<Request>::with_capacity(32),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
            rng: SmallRng::from_entropy(),
            suspicions: VecDeque::new(),
        };
        (gossip, sender)
    }

    pub(crate) fn join(&mut self, member: SocketAddr) -> Result<()> {
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
                //                self.show_metrics();
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
            // FIXME: the disseminated members should be updated only when Ack is received
            self.alive_disseminated_members.update_members(message.count_alive());
        }
    }

    fn recv_letter(&mut self) -> Option<IncomingLetter> {
        match self.server.as_ref().unwrap().recv_from(&mut self.recv_buffer) {
            Ok((count, sender)) => {
                let message = match decode_message(&self.recv_buffer[..count]) {
                    Ok(message) => message,
                    Err(e) => {
                        warn!("Failed to decode from message {:#?}: {}", sender, e);
                        return None;
                    }
                };
                let letter = IncomingLetter { sender, message };
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
                self.alive_disseminated_members.add_member(member);
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
            self.alive_disseminated_members.remove_member(*member);
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
                self.update_state(&letter);
                match letter.message {
                    IncomingMessage::Ping(m) => self.handle_ping(&letter.sender, &m),
                    IncomingMessage::Ack(m) => self.handle_ack(&letter.sender, &m),
                    IncomingMessage::PingRequest(m) => self.handle_indirect_ping(&letter.sender, &m),
                }
            }
        } else if event.readiness().is_writable() {
            if let Some(request) = self.requests.pop_front() {
                debug!("{:?}", request);
                match request {
                    Request::Ping(ref header) => {
                        let mut message = Message::create(MessageType::Ping, header.sequence_number);
                        // FIXME pick members with the lowest recently visited counter (mark to not starve the ones with highest visited counter)
                        // as that may lead to late failure discovery
                        message.with_members(
                            &self
                                .alive_disseminated_members
                                .get_members()
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
                            .alive_disseminated_members
                            .get_members()
                            // do not send the message to the member that is being suspected
                            .filter(|&m| *m != header.target)
                            .take(self.config.num_indirect as usize)
                            .cloned()
                            .collect::<Vec<_>>();
                        for member in indirect_members {
                            let mut message = Message::create(MessageType::PingIndirect, header.sequence_number);
                            // filter is needed to not include target node on the alive list as it is being suspected
                            message.with_members(
                                &std::iter::once(&header.target)
                                    .chain(
                                        self.alive_disseminated_members
                                            .get_members()
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
                        let mut message = Message::create(MessageType::Ping, header.sequence_number);
                        message.with_members(
                            &self
                                .alive_disseminated_members
                                .get_members()
                                .filter(|&member| *member != header.target)
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ack(header) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number);
                        message.with_members(
                            &self
                                .alive_disseminated_members
                                .get_members()
                                .cloned()
                                .collect::<Vec<_>>(),
                            &self.dead_members.iter().cloned().collect::<Vec<_>>(),
                        );
                        self.send_message(&header.target, &message);
                    }
                    Request::AckIndirect(header, member) => {
                        let mut message = Message::create(MessageType::PingAck, header.sequence_number);
                        message.with_members(
                            &std::iter::once(&member)
                                .chain(self.alive_disseminated_members.get_members())
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

    fn update_state(&mut self, message: &DisseminationMessage) {
        match message.message.get_type() {
            MessageType::PingIndirect => self.update_members(
                message
                    .message
                    .get_alive_members()
                    .into_iter()
                    .skip(1)
                    .chain(std::iter::once(message.sender)),
                message.message.get_dead_members().into_iter(),
            ),
            _ => self.update_members(
                message
                    .message
                    .get_alive_members()
                    .into_iter()
                    .chain(std::iter::once(message.sender)),
                message.message.get_dead_members().into_iter(),
            ),
        }
    }

    fn handle_ack(&mut self, sender: &SocketAddr, message: &DisseminationMessage) {
        for ack in self.acks.drain(..).collect::<Vec<_>>() {
            match ack.request {
                Request::PingIndirect(ref header) => {
                    if *sender == header.target && message.sequence_number == header.sequence_number {
                        continue;
                    }
                }
                Request::PingProxy(ref header, ref reply_to) => {
                    if *sender == header.target && message.sequence_number == header.sequence_number {
                        self.requests.push_back(Request::AckIndirect(
                            Header {
                                target: *reply_to,
                                sequence_number: message.sequence_number,
                            },
                            *sender,
                        ));
                        continue;
                    }
                }
                Request::Ping(ref header) => {
                    if *sender == header.target && message.sequence_number == header.sequence_number {
                        continue;
                    }
                }
                _ => unreachable!(),
            }
            self.acks.push(ack);
        }
    }

    fn handle_ping(&mut self, sender: &SocketAddr, message: &DisseminationMessage) {
        self.requests.push_back(Request::Ack(Header {
            target: *sender,
            sequence_number: message.sequence_number,
        }));
    }

    fn handle_indirect_ping(&mut self, sender: &SocketAddr, message: &PingRequestMessage) {
        self.requests.push_back(Request::PingProxy(
            Header {
                target: message.target,
                sequence_number: message.sequence_number,
            },
            *sender,
        ));
    }
}
