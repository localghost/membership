#![deny(missing_docs)]

use crate::disseminated::Disseminated;
use crate::incoming_message::{DisseminationMessageIn, IncomingMessage, PingRequestMessageIn};
use crate::member::{Member, MemberId};
use crate::message::MessageType;
use crate::message_decoder::decode_message;
use crate::message_encoder::{DisseminationMessageEncoder, OutgoingMessage, PingRequestMessageEncoder};
use crate::notification::Notification;
use crate::result::Result;
use crate::suspicion::Suspicion;
use crate::ProtocolConfig;
use failure::{format_err, ResultExt};
use mio::net::UdpSocket;
use mio::{Event, Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel::{Receiver, Sender};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use slog::{debug, info, warn};
use std::collections::{HashMap, VecDeque};
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
    member_id: MemberId,
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
struct PingProxyRequest {
    sender: Member,
    target: Member,
    sequence_number: u64,
}

#[derive(Debug)]
struct AckIndirectRequest {
    target: Member,
    sequence_number: u64,
}

#[derive(Debug)]
enum Request {
    Init(SocketAddr),
    Ping(Header),
    PingIndirect(Header),
    PingProxy(PingProxyRequest),
    Ack(Header),
    AckIndirect(AckIndirectRequest),
}

#[derive(Debug)]
pub(crate) enum ChannelMessage {
    Stop,
    GetMembers(std::sync::mpsc::Sender<Vec<SocketAddr>>),
}

// Unfortunately SyncNode needs to be passed explicitly, it cannot be captured by closure.
struct Timeout<F: FnOnce(&mut SyncNode)> {
    when: std::time::Instant,
    what: F,
}

/// Runs the protocol on current thread, blocking it.
pub(crate) struct SyncNode {
    config: ProtocolConfig,
    udp: Option<UdpSocket>,
    ping_order: Vec<MemberId>,
    broadcast: Disseminated<MemberId>,
    notifications: Disseminated<Notification>,
    members: HashMap<MemberId, Member>,
    dead_members: HashMap<MemberId, Member>,
    next_member_index: usize,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: Vec<u8>,
    myself: Member,
    requests: VecDeque<Request>,
    receiver: Receiver<ChannelMessage>,
    acks: Vec<Ack>,
    rng: SmallRng,
    suspicions: VecDeque<Suspicion>,
    timeouts: Vec<Timeout<Box<dyn FnOnce(&mut SyncNode) + Send>>>,
    logger: slog::Logger,
}

impl SyncNode {
    pub(crate) fn new(bind_address: SocketAddr, config: ProtocolConfig) -> (SyncNode, Sender<ChannelMessage>) {
        let (sender, receiver) = mio_extras::channel::channel();
        let gossip = SyncNode {
            config,
            udp: None,
            ping_order: vec![],
            broadcast: Disseminated::new(),
            notifications: Disseminated::new(),
            members: HashMap::new(),
            dead_members: HashMap::new(),
            next_member_index: 0,
            epoch: 0,
            sequence_number: 0,
            recv_buffer: vec![0u8; 1500],
            myself: Member::new(bind_address),
            requests: VecDeque::<Request>::with_capacity(32),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
            rng: SmallRng::from_entropy(),
            suspicions: VecDeque::new(),
            timeouts: Vec::new(),
            logger: slog::Logger::root(slog::Discard, slog::o!()),
        };
        (gossip, sender)
    }

    pub(crate) fn set_logger(&mut self, logger: slog::Logger) {
        self.logger = logger.new(slog::o!("id" => self.myself.id.to_string()));
    }

    pub(crate) fn start(&mut self) -> Result<()> {
        let poll = Poll::new().unwrap();
        poll.register(&self.receiver, Token(1), Ready::readable(), PollOpt::empty())?;
        self.bind(&poll)?;

        let mut events = Events::with_capacity(1024);
        let mut last_epoch_time = std::time::Instant::now();

        'mainloop: loop {
            poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
            for event in events.iter() {
                match event.token() {
                    Token(0) => {
                        if let Err(e) = self.handle_protocol_event(&event) {
                            slog::warn!(self.logger, "Failed to process protocol event: {:?}", e);
                        }
                    }
                    Token(1) => match self.receiver.try_recv() {
                        Ok(message) => {
                            debug!(self.logger, "ChannelMessage::{:?}", message);
                            match message {
                                ChannelMessage::Stop => {
                                    break 'mainloop;
                                }
                                ChannelMessage::GetMembers(sender) => {
                                    let members = std::iter::once(&self.myself.address)
                                        .chain(self.members.values().map(|m| &m.address))
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if let Err(e) = sender.send(members) {
                                        warn!(self.logger, "Failed to send list of members: {:?}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!(self.logger, "Not ready yet: {:?}", e);
                        }
                    },
                    _ => unreachable!(),
                }
            }

            self.handle_acks()?;

            self.drain_timeout_suspicions()
                .into_iter()
                .for_each(|s| self.handle_timeout_suspicion(&s));

            let now = std::time::Instant::now();
            if now > (last_epoch_time + Duration::from_secs(self.config.protocol_period)) {
                //                self.show_metrics();
                debug!(self.logger, "Notifications: {:?}", self.notifications);
                debug!(self.logger, "Broadcast: {:?}", self.broadcast);

                self.advance_epoch();
                last_epoch_time = now;
            }

            self.handle_timeouts();
        }

        Ok(())
    }

    fn handle_acks(&mut self) -> Result<()> {
        let now = std::time::Instant::now();
        let ack_timeout = Duration::from_secs(self.config.ack_timeout as u64);
        let (handle, postpone): (Vec<_>, Vec<_>) = self
            .acks
            .drain(..)
            .partition(|ack| ack.request_time + ack_timeout <= now);
        handle.into_iter().try_for_each(|ack| self.handle_timeout_ack(ack))?;
        self.acks = postpone;
        Ok(())
    }

    fn handle_timeouts(&mut self) {
        let now = std::time::Instant::now();
        let (handle, postpone): (Vec<_>, Vec<_>) = self.timeouts.drain(..).partition(|t| t.when <= now);
        handle.into_iter().for_each(|t| (t.what)(self));
        self.timeouts = postpone;
    }

    fn drain_timeout_suspicions(&mut self) -> Vec<Suspicion> {
        let mut suspicions = Vec::new();
        loop {
            match self.suspicions.front() {
                Some(ref suspicion) => {
                    if std::time::Instant::now()
                        > (suspicion.created + Duration::from_secs(self.config.suspect_timeout as u64))
                    {
                        suspicions.push(self.suspicions.pop_front().unwrap());
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
        suspicions
    }

    pub(crate) fn join(&mut self, member: SocketAddr) -> Result<()> {
        assert_ne!(member, self.myself.address, "Can't join yourself");
        self.requests.push_front(Request::Init(member));
        self.start()
    }

    fn handle_timeout_suspicion(&mut self, suspicion: &Suspicion) {
        // Check if the `suspicion` is in notifications. Assume that if it is not then
        // the member has already been moved to a different state and this `suspicion` can be dropped.
        let position = self
            .notifications
            .iter()
            .position(|n| n.is_suspect() && *n.member() == suspicion.member);
        if let Some(position) = position {
            self.notifications.remove(position);
            self.notifications.add(Notification::Confirm {
                member: self.members[&suspicion.member.id].clone(),
            });
            self.handle_confirm(&self.members[&suspicion.member.id].clone())
        } else {
            debug!(self.logger, "Member {} already removed.", suspicion.member.id);
        }
    }

    fn advance_epoch(&mut self) {
        if let Some(member_id) = self.get_next_member() {
            let ping = Request::Ping(Header {
                member_id,
                sequence_number: self.get_next_sequence_number(),
            });
            self.requests.push_front(ping);
        }
        self.epoch += 1;
        info!(self.logger, "New epoch: {}", self.epoch);
    }

    fn handle_timeout_ack(&mut self, ack: Ack) -> Result<()> {
        match ack.request {
            Request::Init(address) => {
                info!(self.logger, "Failed to join {}", address);
                self.timeouts.push(Timeout {
                    when: std::time::Instant::now() + std::time::Duration::from_secs(self.config.join_retry_timeout),
                    what: Box::new(|myself| myself.requests.push_front(ack.request)),
                });
            }
            Request::Ping(header) => {
                self.requests.push_back(Request::PingIndirect(header));
            }
            Request::PingIndirect(header) => {
                self.handle_suspect(header.member_id);
            }
            Request::PingProxy(request) => {
                warn!(
                    self.logger,
                    "Ping proxy from {} to {} timed out", request.sender.id, request.target.id
                );
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn bind(&mut self, poll: &Poll) -> Result<()> {
        self.udp = Some(UdpSocket::bind(&self.myself.address).context("Failed to bind UDP socket")?);
        // FIXME: change to `PollOpt::edge()`
        poll.register(
            self.udp.as_ref().unwrap(),
            Token(0),
            Ready::readable() | Ready::writable(),
            PollOpt::level(),
        )
        .map_err(|e| format_err!("Failed to register UDP socket for polling: {:?}", e))
    }

    fn send_message(&mut self, target: SocketAddr, message: OutgoingMessage) {
        debug!(self.logger, "{:?} <- {:?}", target, message);
        // This can happen if this node is returning to a group before the group noticing that the node's previous
        // instance has died.
        // FIXME: this is not the best place for this, preferably it should be handled when receiving a message
        // with member ID not matching oneself.
        if target == self.myself.address {
            debug!(self.logger, "Trying to send a message to myself, dropping it");
            return;
        }
        match self.udp.as_ref().unwrap().send_to(message.buffer(), &target) {
            Err(e) => warn!(self.logger, "Message to {:?} was not delivered due to {:?}", target, e),
            Ok(count) => {
                debug!(self.logger, "Send {} bytes", count);
                if let OutgoingMessage::DisseminationMessage(ref dissemination_message) = message {
                    self.notifications.mark(dissemination_message.num_notifications());
                    self.broadcast.mark(dissemination_message.num_broadcast());
                }
            }
        }
    }

    fn recv_letter(&mut self) -> Option<IncomingLetter> {
        match self.udp.as_ref().unwrap().recv_from(&mut self.recv_buffer) {
            Ok((count, sender)) => {
                debug!(self.logger, "Received {} bytes from {:?}", count, sender);
                let message = match decode_message(&self.recv_buffer[..count]) {
                    Ok(message) => message,
                    Err(e) => {
                        warn!(self.logger, "Failed to decode from message {:#?}: {}", sender, e);
                        return None;
                    }
                };
                let letter = IncomingLetter { sender, message };
                debug!(self.logger, "{:?}", letter);
                Some(letter)
            }
            Err(e) => {
                warn!(self.logger, "Failed to receive letter due to {:?}", e);
                None
            }
        }
    }

    fn update_members<'m>(&mut self, members: impl Iterator<Item = &'m Member>) {
        for member in members {
            self.update_member(member);
        }
    }

    fn update_member(&mut self, member: &Member) {
        if self.dead_members.contains_key(&member.id) {
            info!(self.logger, "Member {:?} has already been marked as dead", member);
            return;
        }
        if member.id != self.myself.id && self.members.insert(member.id, member.clone()).is_none() {
            info!(self.logger, "Member joined: {:?}", member);
            self.ping_order.push(member.id);
            self.broadcast.add(member.id);
        }
    }

    fn update_notifications<'m>(&mut self, notifications: impl Iterator<Item = &'m Notification>) {
        // TODO: check this does not miss notifications with not yet seen members
        // TODO: remove from self.notifications those notifications that are overridden by the new ones
        for notification in notifications {
            if self.notifications.iter().find(|&n| n >= notification).is_some() {
                continue;
            }
            match notification {
                Notification::Confirm { member } => self.handle_confirm(member),
                Notification::Alive { member } => self.handle_alive(member),
                Notification::Suspect { member } => self.handle_suspect(member.id),
            }
            let obsolete_notifications = self
                .notifications
                .iter()
                .filter(|&n| n < notification)
                .cloned()
                .collect::<Vec<_>>();
            for n in obsolete_notifications {
                self.notifications.remove_item(&n);
            }
            // Suspect notification does not have a limit because it can be dropped only when it is moved to
            // Confirm or Alive. Suspect is limited by the respective Suspicion timeout.
            if notification.is_suspect() {
                self.notifications.add((*notification).clone());
            } else {
                self.notifications.add_with_limit((*notification).clone(), 20);
            }
        }
    }

    fn handle_confirm(&mut self, member: &Member) {
        if let Some(position) = self.suspicions.iter().position(|s| s.member == *member) {
            self.suspicions.remove(position);
        }
        self.dead_members.insert(member.id, member.clone());
        self.remove_member(&member.id);
    }

    fn handle_alive(&mut self, member: &Member) {
        self.update_member(member);
    }

    fn handle_suspect(&mut self, member_id: MemberId) {
        match self.members.get(&member_id) {
            Some(member) => {
                // FIXME: Might be inefficient to check entire deq
                if self.suspicions.iter().find(|s| s.member == *member).is_none() {
                    info!(self.logger, "Start suspecting member {:?}", member);
                    self.suspicions.push_back(Suspicion::new(member.clone()));
                    self.notifications.add(Notification::Suspect { member: member.clone() });
                }
            }
            None => debug!(self.logger, "Trying to suspect unknown member {:?}", member_id),
        }
    }

    fn remove_member(&mut self, member_id: &MemberId) {
        if let Some(removed_member) = self.members.remove(member_id) {
            let idx = self.ping_order.iter().position(|e| e == member_id).unwrap();
            self.ping_order.remove(idx);
            self.broadcast.remove_item(member_id);
            if idx <= self.next_member_index && self.next_member_index > 0 {
                self.next_member_index -= 1;
            }
            info!(self.logger, "Member removed: {:?}", removed_member);
        }
    }

    fn get_next_member(&mut self) -> Option<MemberId> {
        if self.ping_order.is_empty() {
            return None;
        }
        // Following SWIM paper, section 4.3, next member to probe is picked in round-robin fashion, with all
        // the members randomly shuffled after each one has been probed.
        // FIXME: one thing that is missing is that new members are always added at the end instead of at uniformly
        // random position.
        if self.next_member_index == 0 {
            self.ping_order.shuffle(&mut self.rng);
        }
        let target = self.ping_order[self.next_member_index];
        self.next_member_index = (self.next_member_index + 1) % self.ping_order.len();
        Some(target)
    }

    fn get_next_sequence_number(&mut self) -> u64 {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        sequence_number
    }

    fn handle_protocol_event(&mut self, event: &Event) -> Result<()> {
        if event.readiness().is_readable() {
            if let Some(letter) = self.recv_letter() {
                match letter.message {
                    IncomingMessage::Ping(m) => self.handle_ping(&m),
                    IncomingMessage::Ack(m) => self.handle_ack(&m),
                    IncomingMessage::PingRequest(m) => self.handle_indirect_ping(&m),
                }
            }
        } else if event.readiness().is_writable() {
            if let Some(request) = self.requests.pop_front() {
                debug!(self.logger, "{:?}", request);
                match request {
                    Request::Init(address) => {
                        let message = DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::Ping)?
                            .sender(&self.myself)?
                            .sequence_number(0)?
                            .encode();
                        self.send_message(address, message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ping(ref header) if self.members.contains_key(&header.member_id) => {
                        let message = DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::Ping)?
                            .sender(&self.myself)?
                            .sequence_number(header.sequence_number)?
                            .notifications(self.notifications.iter())?
                            .broadcast(self.broadcast.iter().map(|id| &self.members[id]))?
                            .encode();
                        self.send_message(self.members[&header.member_id].address, message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ping(ref header) => {
                        info!(
                            self.logger,
                            "Dropping Ping message, member {} has already been removed.", header.member_id
                        );
                    }
                    Request::PingIndirect(ref header) if self.members.contains_key(&header.member_id) => {
                        let indirect_members = self
                            .members
                            .keys()
                            .filter(|&key| *key != header.member_id)
                            .take(self.config.num_indirect as usize)
                            .cloned()
                            .collect::<Vec<_>>();
                        indirect_members.iter().try_for_each(|member_id| -> Result<()> {
                            let message = PingRequestMessageEncoder::new()
                                .sender(&self.myself)?
                                .sequence_number(header.sequence_number)?
                                .target(&self.members[&header.member_id])?
                                .encode();
                            self.send_message(self.members[member_id].address, message);
                            Ok(())
                        })?;
                        self.acks.push(Ack::new(request));
                    }
                    Request::PingIndirect(ref header) => {
                        info!(
                            self.logger,
                            "Dropping PingIndirect message, member {} has already been removed.", header.member_id
                        );
                    }
                    Request::PingProxy(ref ping_proxy) => {
                        let message = DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::Ping)?
                            .sender(&self.myself)?
                            .sequence_number(ping_proxy.sequence_number)?
                            .notifications(self.notifications.iter())?
                            .broadcast(self.broadcast.iter().map(|id| &self.members[id]))?
                            .encode();
                        self.send_message(ping_proxy.target.address, message);
                        self.acks.push(Ack::new(request));
                    }
                    Request::Ack(header) => {
                        let message = DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::PingAck)?
                            .sender(&self.myself)?
                            .sequence_number(header.sequence_number)?
                            .notifications(self.notifications.iter())?
                            .broadcast(self.broadcast.iter().map(|id| &self.members[id]))?
                            .encode();
                        self.send_message(self.members[&header.member_id].address, message);
                    }
                    Request::AckIndirect(ack_indirect) => {
                        let message = DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::PingAck)?
                            .sender(&self.myself)?
                            .sequence_number(ack_indirect.sequence_number)?
                            .encode();
                        self.send_message(ack_indirect.target.address, message);
                    }
                }
            }
        }
        Ok(())
    }

    fn update_state(&mut self, message: &DisseminationMessageIn) {
        self.update_notifications(message.notifications.iter());
        self.update_member(&message.sender);
        self.update_members(message.broadcast.iter());
    }

    fn handle_ack(&mut self, message: &DisseminationMessageIn) {
        for ack in self.acks.drain(..).collect::<Vec<_>>() {
            match ack.request {
                Request::Init(address) => {
                    self.update_state(message);
                    if message.sender.address != address || message.sequence_number != 0 {
                        panic!("Initial ping request failed, unable to continue");
                    }
                    continue;
                }
                Request::PingIndirect(ref header) => {
                    self.update_state(message);
                    if message.sender.id == header.member_id && message.sequence_number == header.sequence_number {
                        continue;
                    }
                }
                Request::PingProxy(ref ping_proxy) => {
                    if message.sender.id == ping_proxy.target.id
                        && message.sequence_number == ping_proxy.sequence_number
                    {
                        self.requests.push_back(Request::AckIndirect(AckIndirectRequest {
                            target: ping_proxy.sender.clone(),
                            sequence_number: ping_proxy.sequence_number,
                        }));
                        continue;
                    }
                }
                Request::Ping(ref header) => {
                    self.update_state(message);
                    if message.sender.id == header.member_id && message.sequence_number == header.sequence_number {
                        continue;
                    }
                }
                _ => unreachable!(),
            }
            self.acks.push(ack);
        }
    }

    fn handle_ping(&mut self, message: &DisseminationMessageIn) {
        self.update_state(message);
        self.requests.push_back(Request::Ack(Header {
            member_id: message.sender.id,
            sequence_number: message.sequence_number,
        }));
    }

    fn handle_indirect_ping(&mut self, message: &PingRequestMessageIn) {
        self.requests.push_back(Request::PingProxy(PingProxyRequest {
            sender: message.sender.clone(),
            target: message.target.clone(),
            sequence_number: message.sequence_number,
        }));
    }
}
