#![deny(missing_docs)]

use crate::disseminated::Disseminated;
use crate::incoming_message::{DisseminationMessageIn, IncomingMessage, PingRequestMessageIn};
use crate::member::{Member, MemberId};
use crate::members::Members;
use crate::message::MessageType;
use crate::message_decoder::decode_message;
use crate::message_encoder::{DisseminationMessageEncoder, OutgoingMessage, PingRequestMessageEncoder};
use crate::messenger::{IncomingLetter, Messenger, MessengerImpl, OutgoingLetter};
use crate::monotonic_clock::MonotonicClock;
use crate::notification::Notification;
use crate::result::Result;
use crate::suspicion::Suspicion;
use crate::ProtocolConfig;
use anyhow::Context;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime;
use tokio::sync;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

// struct IncomingLetter {
//     sender: SocketAddr,
//     message: IncomingMessage,
// }

// impl fmt::Debug for IncomingLetter {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "IncomingLetter {{ sender: {:#?}, message: {:#?} }}",
//             self.sender, self.message
//         )
//     }
// }

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
    GetMembers(std::sync::mpsc::SyncSender<Vec<SocketAddr>>),
}

// Unfortunately SyncNode needs to be passed explicitly, it cannot be captured by closure.
struct Timeout<F: FnOnce(&mut SyncNode)> {
    when: std::time::Instant,
    what: F,
}

struct SyncNodeInner {
    config: ProtocolConfig,
    udp: Option<UdpSocket>,
    notifications: Disseminated<Notification>,
    epoch: u64,
    sequence_number: u64,
    recv_buffer: Vec<u8>,
    myself: Member,
    requests: VecDeque<Request>,
    receiver: tokio::sync::mpsc::Receiver<ChannelMessage>,
    acks: Vec<Ack>,
    suspicions: VecDeque<Suspicion>,
    timeouts: Vec<Timeout<Box<dyn FnOnce(&mut SyncNode) + Send>>>,
    logger: slog::Logger,
    members: Members,
}

impl SyncNodeInner {
    // async fn handle_ping(&mut self, message: DisseminationMessageIn) -> Result<()> {
    //     self.update_state(&message);
    //     let response = DisseminationMessageEncoder::new(1024)
    //         .message_type(MessageType::PingAck)?
    //         .sender(&self.myself)?
    //         .sequence_number(message.sequence_number)?
    //         .notifications(self.notifications.for_dissemination())?
    //         .broadcast(self.members.for_broadcast())?
    //         .encode();
    //     self.sender
    //         .as_mut()
    //         .unwrap()
    //         .send(response)
    //         .await
    //         .with_context(|| format!("Failed to send ACK for Ping"))
    //     // self.requests.push_back(Request::Ack(Header {
    //     //     member_id: message.sender.id,
    //     //     sequence_number: message.sequence_number,
    //     // }));
    // }

    async fn handle_ack(&mut self, message: DisseminationMessageIn) -> Result<()> {
        for ack in self.acks.drain(..).collect::<Vec<_>>() {
            match ack.request {
                Request::Init(address) => {
                    self.update_state(&message);
                    if message.sender.address != address || message.sequence_number != 0 {
                        panic!("Initial ping request failed, unable to continue");
                    }
                    continue;
                }
                Request::PingIndirect(ref header) => {
                    self.update_state(&message);
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
                    self.update_state(&message);
                    if message.sender.id == header.member_id && message.sequence_number == header.sequence_number {
                        continue;
                    }
                }
                _ => unreachable!(),
            }
            self.acks.push(ack);
        }
        Ok(())
    }

    async fn handle_indirect_ping(&mut self, message: PingRequestMessageIn) -> Result<()> {
        Ok(())
    }

    // async fn handle_incoming_message(&mut self, letter: IncomingLetter) -> Result<()> {
    //     match letter.message {
    //         IncomingMessage::Ping(m) => self.handle_ping(m).await,
    //         IncomingMessage::Ack(m) => self.handle_ack(m).await,
    //         IncomingMessage::PingRequest(m) => self.handle_indirect_ping(m).await,
    //     }
    // }

    fn update_state(&mut self, message: &DisseminationMessageIn) {
        self.update_members(std::iter::once(&message.sender).chain(message.broadcast.iter()));
        self.process_notifications(message.notifications.iter());
    }

    fn update_members<'m>(&mut self, members: impl Iterator<Item = &'m Member>) {
        for member in members {
            self.update_member(member);
        }
    }

    fn update_member(&mut self, member: &Member) {
        if member.id == self.myself.id {
            return;
        }
        // This can happen if this node is returning to a group before the group noticing that the node's previous
        // instance has died.
        if member.address == self.myself.address {
            warn!("Trying to add myself but with wrong ID {:?}", member);
            return;
        }
        match self.members.add_or_update(member.clone()) {
            Ok(_) => info!("Member joined: {:?}", member),
            Err(e) => warn!("Adding new member failed due to: {}", e),
        }
    }

    fn process_notifications<'m>(&mut self, notifications: impl Iterator<Item = &'m Notification>) {
        for notification in notifications {
            if self.notifications.iter().find(|&n| n >= notification).is_some() {
                continue;
            }
            match notification {
                Notification::Confirm { member } => self.handle_confirm(member),
                Notification::Alive { member } => self.handle_alive(member),
                Notification::Suspect { member } => self.handle_suspect(member),
            }
            let obsolete_notifications = self
                .notifications
                .for_dissemination()
                .filter(|&n| n < notification)
                .cloned()
                .collect::<Vec<_>>();
            for n in obsolete_notifications {
                self.remove_notification(&n);
            }
            self.add_notification(notification.clone());
        }
    }

    fn remove_notification(&mut self, notification: &Notification) {
        if notification.is_suspect() {
            self.remove_suspicion(notification.member());
        }
        self.notifications.remove_item(notification);
    }

    fn add_notification(&mut self, notification: Notification) {
        // Suspect notification does not have a limit because it can be dropped only when it is moved to
        // Confirm or Alive. Suspect is limited by the respective Suspicion timeout.
        if notification.is_suspect() {
            self.notifications.add(notification);
        } else {
            self.notifications
                .add_with_limit(notification, self.config.notification_dissemination_times);
        }
    }

    fn handle_confirm(&mut self, member: &Member) {
        self.remove_suspicion(member);
        match self.members.remove(&member.id) {
            Ok(_) => info!("Member {:?} removed", member),
            Err(e) => warn!("Member {:?} was not removed due to: {}", member, e),
        }
    }

    fn remove_suspicion(&mut self, member: &Member) {
        if let Some(position) = self.suspicions.iter().position(|s| s.member == *member) {
            self.suspicions.remove(position);
        }
    }

    fn handle_alive(&mut self, member: &Member) {
        self.remove_suspicion(member);
        self.update_member(member);
    }

    fn handle_suspect(&mut self, member: &Member) {
        if member.id == self.myself.id {
            self.handle_suspect_myself(member);
        } else {
            self.handle_suspect_other(member);
            self.update_member(member);
        }
    }

    fn handle_suspect_myself(&mut self, suspect: &Member) {
        if self.myself.incarnation <= suspect.incarnation {
            self.myself.incarnation = suspect.incarnation + 1;
            info!(
                "I am being suspected, increasing my incarnation to {}",
                self.myself.incarnation
            );
            self.add_notification(Notification::Alive {
                member: self.myself.clone(),
            });
        }
    }

    fn handle_suspect_other(&mut self, suspect: &Member) {
        // FIXME: Might be inefficient to check entire deq
        match self.suspicions.iter().position(|s| s.member.id == suspect.id) {
            Some(idx) if self.suspicions[idx].member.incarnation >= suspect.incarnation => {
                info!("Member {:?} is already suspected", self.suspicions[idx].member);
            }
            Some(idx) => {
                info!(
                    "Member {:?} suspected with lower incarnation, replacing it",
                    self.suspicions[idx].member
                );
                self.suspicions.remove(idx);
                self.suspect_member(suspect)
            }
            None => self.suspect_member(suspect),
        }
    }

    fn suspect_member(&mut self, suspect: &Member) {
        info!("Start suspecting member {:?}", suspect);
        let member = suspect.clone();
        self.suspicions.push_back(Suspicion::new(suspect.clone()));
        self.add_notification(Notification::Suspect { member });
    }

    fn advance_epoch(&mut self) {
        if let Some(member_id) = self.members.next() {
            let ping = Request::Ping(Header {
                member_id,
                sequence_number: self.get_next_sequence_number(),
            });
            self.requests.push_front(ping);
        }
        self.epoch += 1;
        info!("New epoch: {}", self.epoch);
    }

    fn get_next_sequence_number(&mut self) -> u64 {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        sequence_number
    }
}

#[derive(Clone)]
struct ProtocolContext {
    state: Arc<Mutex<SyncNodeInner>>,
    sender: Sender<OutgoingLetter>,
}

impl Debug for ProtocolContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolContext").finish()
    }
}

struct ProtocolRunner {
    context: ProtocolContext,
    receiver: Receiver<IncomingLetter>,
    protocol_interval: tokio::time::Interval,
    ack_timeout: Duration,
    ack_interval: tokio::time::Interval,
}

impl ProtocolRunner {
    fn new(
        state: Arc<Mutex<SyncNodeInner>>,
        sender: Sender<OutgoingLetter>,
        receiver: Receiver<IncomingLetter>,
    ) -> Self {
        let protocol_period = Duration::from_secs(state.lock().unwrap().config.protocol_period);
        let ack_timeout = Duration::from_secs(state.lock().unwrap().config.ack_timeout as u64);
        Self {
            context: ProtocolContext { state, sender },
            receiver: receiver,
            protocol_interval: tokio::time::interval(protocol_period),
            ack_timeout: ack_timeout,
            ack_interval: tokio::time::interval(ack_timeout),
        }
    }

    // async fn run(&mut self) {
    //     info!("Starting ProtocolRunner");
    //     let epoch_advancer = EpochAdvancer {
    //         context: self.context.clone(),
    //     };
    //     let ack_handler = AckHandler {
    //         context: self.context.clone(),
    //     };
    //     // let mut message_handler = MessageHandler {
    //     //     context: self.context.clone(),
    //     //     receiver: self.receiver.take().unwrap(),
    //     // };
    //     info!("Spawned the actors and waiting for them...");
    //     tokio::join!(
    //         tokio::spawn(async move { epoch_advancer.run().await }),
    //         tokio::spawn(async move { ack_handler.run().await }),
    //         // tokio::spawn(async move { message_handler.run().await })
    //     );
    // }

    async fn run2(&mut self) {
        loop {
            tokio::select!(
                _ = self.protocol_interval.tick() => {
                    self.advance_epoch().await.expect("advancing epoch failed")
                }
                _ = self.ack_interval.tick() => {
                    self.handle_acks()
                }
                Some(letter) = self.receiver.recv() => {
                    todo!()
                }
            )
        }
    }

    async fn advance_epoch(&mut self) -> Result<()> {
        let next_member = self.context.state.lock().unwrap().members.next();
        debug!(next_member = tracing::field::debug(next_member));
        match next_member {
            Some(member_id) => {
                let next_sequence_number = self.context.state.lock().unwrap().get_next_sequence_number();
                debug!(next_sequence_number = next_sequence_number);
                let letter = {
                    let mut state = self.context.state.lock().unwrap();
                    let message = {
                        DisseminationMessageEncoder::new(1024)
                            .message_type(MessageType::Ping)?
                            .sender(&state.myself)?
                            .sequence_number(next_sequence_number)?
                            .notifications(state.notifications.for_dissemination())?
                            .broadcast(state.members.for_broadcast())?
                            .encode()
                    };
                    OutgoingLetter {
                        message,
                        receiver: state.members.get(&member_id).unwrap().address,
                    }
                };
                debug!(letter = tracing::field::debug(&letter));
                self.context.sender.send(letter).await.unwrap();

                let ack = Ack::new(Request::Ping(Header {
                    member_id,
                    sequence_number: next_sequence_number,
                }));
                self.context.state.lock().unwrap().acks.push(ack);
            }
            None => {}
        }
        Ok(())
    }

    fn handle_acks(&mut self) {
        let now = std::time::Instant::now();
        let acks = &mut self.context.state.lock().unwrap().acks;
        let (handle, postpone): (Vec<_>, Vec<_>) = acks
            .drain(..)
            .partition(|ack| ack.request_time + self.ack_timeout <= now);
        // handle.into_iter().for_each(|ack| self.handle_timeout_ack(ack));
        self.context.state.lock().unwrap().acks = postpone;
    }
}

struct MessageHandler {
    context: ProtocolContext,
    receiver: Receiver<IncomingLetter>,
}

impl MessageHandler {
    async fn run(&mut self) -> Result<()> {
        loop {
            match self.receiver.recv().await {
                Some(letter) => todo!(), //self.handle_incoming_message(letter),
                None => todo!(),
            }
        }
    }

    async fn handle_incoming_message(&mut self, letter: IncomingLetter) -> Result<()> {
        match letter.message {
            IncomingMessage::Ping(m) => todo!(),        //self.handle_ping(m).await,
            IncomingMessage::Ack(m) => todo!(),         //self.handle_ack(m).await,
            IncomingMessage::PingRequest(m) => todo!(), //self.handle_indirect_ping(m).await,
        }
    }

    // async fn handle_ping(&mut self, message: DisseminationMessageIn) -> Result<()> {
    //     self.update_state(&message);
    //     let mut state = self.inner.lock().unwrap();
    //     let response = DisseminationMessageEncoder::new(1024)
    //         .message_type(MessageType::PingAck)?
    //         .sender(&state.myself)?
    //         .sequence_number(message.sequence_number)?
    //         .notifications(state.notifications.for_dissemination())?
    //         .broadcast(state.members.for_broadcast())?
    //         .encode();
    //     state
    //         .sender
    //         .as_mut()
    //         .unwrap()
    //         .send(response)
    //         .await
    //         .with_context(|| format!("Failed to send ACK for Ping"))
    // }
    //
    // fn update_state(&mut self, message: &DisseminationMessageIn) {
    //     self.update_members(std::iter::once(&message.sender).chain(message.broadcast.iter()));
    //     self.process_notifications(message.notifications.iter());
    // }
    //
    // fn update_members<'m>(&mut self, members: impl Iterator<Item = &'m Member>) {
    //     for member in members {
    //         self.update_member(member);
    //     }
    // }
    //
    // fn update_member(&mut self, member: &Member) {
    //     if member.id == self.myself.id {
    //         return;
    //     }
    //     // This can happen if this node is returning to a group before the group noticing that the node's previous
    //     // instance has died.
    //     if member.address == self.myself.address {
    //         warn!(self.logger, "Trying to add myself but with wrong ID {:?}", member);
    //         return;
    //     }
    //     match self.members.add_or_update(member.clone()) {
    //         Ok(_) => info!(self.logger, "Member joined: {:?}", member),
    //         Err(e) => warn!(self.logger, "Adding new member failed due to: {}", e),
    //     }
    // }
}

// async fn handle_timeout_ack(&mut self, ack: Ack) {
//     match ack.request {
//         Request::Init(address) => {
//             // info!(self.logger, "Failed to join {}", address);
//             self.timeouts.push(Timeout {
//                 when: std::time::Instant::now() + std::time::Duration::from_secs(self.config.join_retry_timeout),
//                 what: Box::new(|myself| myself.requests.push_front(ack.request)),
//             });
//         }
//         Request::Ping(header) => {
//             self.requests.push_back(Request::PingIndirect(header));
//         }
//         Request::PingIndirect(header) => {
//             if let Some(member) = self.members.get(&header.member_id) {
//                 // FIXME: it shouldn't be necessary to clone the member :/
//                 let member = member.clone();
//                 self.handle_suspect_other(&member);
//             } else {
//                 warn!(
//                     self.logger,
//                     "Trying to suspect a member that has already been removed: {}", header.member_id
//                 );
//             }
//         }
//         Request::PingProxy(request) => {
//             warn!(
//                 self.logger,
//                 "Ping proxy from {} to {} timed out", request.sender.id, request.target.id
//             );
//         }
//         _ => unreachable!(),
//     }
// }

struct ProtocolReceiver {
    state: Arc<Mutex<SyncNodeInner>>,
    sender: Sender<OutgoingMessage>,
    receiver: Receiver<IncomingLetter>,
}

impl ProtocolReceiver {
    fn new(
        state: Arc<Mutex<SyncNodeInner>>,
        sender: Sender<OutgoingMessage>,
        receiver: Receiver<IncomingLetter>,
    ) -> Self {
        Self {
            state,
            sender,
            receiver,
        }
    }

    async fn run(&mut self) {
        loop {
            match self.receiver.recv().await {
                Some(letter) => todo!(), // self.handle_incoming_message(letter).await,
                None => todo!(),
            }
        }
    }

    // async fn handle_incoming_message(&mut self, letter: IncomingLetter) -> Result<()> {
    //     match letter.message {
    //         IncomingMessage::Ping(m) => self.handle_ping(m).await,
    //         IncomingMessage::Ack(m) => self.handle_ack(m).await,
    //         IncomingMessage::PingRequest(m) => self.handle_indirect_ping(m).await,
    //     }
    // }
}

/// Runs the protocol on current thread, blocking it.
pub(crate) struct SyncNode {
    inner: Arc<Mutex<SyncNodeInner>>,
    runtime: runtime::Runtime,
}

impl SyncNode {
    pub(crate) fn new(
        bind_address: SocketAddr,
        config: ProtocolConfig,
    ) -> (SyncNode, tokio::sync::mpsc::Sender<ChannelMessage>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let inner = SyncNodeInner {
            config,
            udp: None,
            notifications: Disseminated::new(),
            epoch: 0,
            sequence_number: 0,
            recv_buffer: vec![0u8; 1500],
            myself: Member::new(bind_address),
            requests: VecDeque::<Request>::with_capacity(32),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
            suspicions: VecDeque::new(),
            timeouts: Vec::new(),
            logger: slog::Logger::root(slog::Discard, slog::o!()),
            members: Members::new(),
        };
        let sync_node = Self {
            inner: Arc::new(Mutex::new(inner)),
            runtime: runtime::Builder::new_current_thread().enable_all().build().unwrap(),
        };
        (sync_node, sender)
    }

    pub(crate) fn set_logger(&mut self, logger: slog::Logger) {
        let mut inner = self.inner.lock().unwrap();
        inner.logger = logger.new(slog::o!("id" => inner.myself.id.to_string()));
    }

    // async fn handle_incoming_message(&mut self, letter: IncomingLetter) -> Result<()> {
    //     match letter.message {
    //         IncomingMessage::Ping(m) => self.handle_ping(m).await,
    //         IncomingMessage::Ack(m) => Ok(()),         //self.handle_ack(m).await,
    //         IncomingMessage::PingRequest(m) => Ok(()), //self.handle_indirect_ping(m).await,
    //     }
    // }

    // async fn handle_ping(&mut self, message: DisseminationMessageIn) -> Result<()> {
    //     // self.update_state(&message);
    //     let mut state = self.inner.lock().unwrap();
    //     let response = DisseminationMessageEncoder::new(1024)
    //         .message_type(MessageType::PingAck)?
    //         .sender(&state.myself)?
    //         .sequence_number(message.sequence_number)?
    //         .notifications(state.notifications.for_dissemination())?
    //         .broadcast(state.members.for_broadcast())?
    //         .encode();
    //     state
    //         .sender
    //         .as_mut()
    //         .unwrap()
    //         .send(response)
    //         .await
    //         .with_context(|| format!("Failed to send ACK for Ping"))
    // }

    pub(crate) fn start(&mut self) -> Result<()> {
        let id_value = tracing::field::debug(self.inner.lock().unwrap().myself.id);
        tracing::info_span!("_", id = id_value).in_scope(|| -> Result<()> {
            self.runtime.block_on(async {
                let mut messenger = MessengerImpl::new();
                let (msg_sender, mut msg_receiver) = messenger.start(self.inner.lock().unwrap().myself.address)?;
                let mut protocol_runner = ProtocolRunner::new(self.inner.clone(), msg_sender, msg_receiver);
                // protocol_runner.run().await;
                Ok(())
            })
        })
    }

    // let poll = Poll::new().unwrap();
    // poll.register(&self.receiver, Token(1), Ready::readable(), PollOpt::empty())?;
    // self.bind(&poll)?;
    //
    // let mut events = Events::with_capacity(1024);
    // let mut last_epoch_time = std::time::Instant::now();
    //
    // 'mainloop: loop {
    //     poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
    //     for event in events.iter() {
    //         match event.token() {
    //             Token(0) => {
    //                 if let Err(e) = self.handle_protocol_event(&event) {
    //                     warn!(self.logger, "Failed to process protocol event: {:?}", e);
    //                 }
    //             }
    //             Token(1) => match self.receiver.try_recv() {
    //                 Ok(message) => {
    //                     debug!(self.logger, "ChannelMessage::{:?}", message);
    //                     match message {
    //                         ChannelMessage::Stop => {
    //                             break 'mainloop;
    //                         }
    //                         ChannelMessage::GetMembers(sender) => {
    //                             let members = std::iter::once(&self.myself.address)
    //                                 .chain(self.members.iter().map(|m| &m.address))
    //                                 .cloned()
    //                                 .collect::<Vec<_>>();
    //                             if let Err(e) = sender.send(members) {
    //                                 warn!(self.logger, "Failed to send list of members: {:?}", e);
    //                             }
    //                         }
    //                     }
    //                 }
    //                 Err(e) => {
    //                     debug!(self.logger, "Not ready yet: {:?}", e);
    //                 }
    //             },
    //             _ => unreachable!(),
    //         }
    //     }
    //
    //     self.handle_acks();
    //
    //     self.drain_timeout_suspicions()
    //         .into_iter()
    //         .for_each(|s| self.handle_timeout_suspicion(&s));
    //
    //     let now = std::time::Instant::now();
    //     if now > (last_epoch_time + Duration::from_secs(self.config.protocol_period)) {
    //         self.advance_epoch();
    //         last_epoch_time = now;
    //     }
    //
    //     self.handle_timeouts();
    // }
    //
    // Ok(())
    // }

    // fn handle_acks(&mut self) {
    //     let now = std::time::Instant::now();
    //     let ack_timeout = Duration::from_secs(self.config.ack_timeout as u64);
    //     let (handle, postpone): (Vec<_>, Vec<_>) = self
    //         .acks
    //         .drain(..)
    //         .partition(|ack| ack.request_time + ack_timeout <= now);
    //     handle.into_iter().for_each(|ack| self.handle_timeout_ack(ack));
    //     self.acks = postpone;
    // }
    //
    // fn handle_timeouts(&mut self) {
    //     let now = std::time::Instant::now();
    //     let (handle, postpone): (Vec<_>, Vec<_>) = self.timeouts.drain(..).partition(|t| t.when <= now);
    //     handle.into_iter().for_each(|t| (t.what)(self));
    //     self.timeouts = postpone;
    // }
    //
    // fn drain_timeout_suspicions(&mut self) -> Vec<Suspicion> {
    //     let mut suspicions = Vec::new();
    //     loop {
    //         match self.suspicions.front() {
    //             Some(ref suspicion) => {
    //                 if std::time::Instant::now()
    //                     > (suspicion.created + Duration::from_secs(self.config.suspect_timeout as u64))
    //                 {
    //                     suspicions.push(self.suspicions.pop_front().unwrap());
    //                 } else {
    //                     break;
    //                 }
    //             }
    //             None => break,
    //         }
    //     }
    //     suspicions
    // }
    //
    pub(crate) fn join(&mut self, member: SocketAddr) -> Result<()> {
        // assert_ne!(member, self.myself.address, "Can't join yourself");
        // self.requests.push_front(Request::Init(member));
        // self.start()
        Ok(())
    }
    //
    // fn handle_timeout_suspicion(&mut self, suspicion: &Suspicion) {
    //     // Check if the `suspicion` is in notifications. Assume that if it is not then
    //     // the member has already been moved to a different state and this `suspicion` can be dropped.
    //     let position = self
    //         .notifications
    //         .for_dissemination()
    //         .position(|n| n.is_suspect() && *n.member() == suspicion.member);
    //     if let Some(position) = position {
    //         self.notifications.remove(position);
    //         self.notifications.add(Notification::Confirm {
    //             member: self.members.get(&suspicion.member.id).unwrap().clone(),
    //         });
    //         self.handle_confirm(&self.members.get(&suspicion.member.id).unwrap().clone())
    //     } else {
    //         debug!(self.logger, "Member {} already removed.", suspicion.member.id);
    //     }
    // }
    //
    // fn advance_epoch(&mut self) {
    //     if let Some(member_id) = self.members.next() {
    //         let ping = Request::Ping(Header {
    //             member_id,
    //             sequence_number: self.get_next_sequence_number(),
    //         });
    //         self.requests.push_front(ping);
    //     }
    //     self.epoch += 1;
    //     info!(self.logger, "New epoch: {}", self.epoch);
    // }
    //
    // fn handle_timeout_ack(&mut self, ack: Ack) {
    //     match ack.request {
    //         Request::Init(address) => {
    //             info!(self.logger, "Failed to join {}", address);
    //             self.timeouts.push(Timeout {
    //                 when: std::time::Instant::now() + std::time::Duration::from_secs(self.config.join_retry_timeout),
    //                 what: Box::new(|myself| myself.requests.push_front(ack.request)),
    //             });
    //         }
    //         Request::Ping(header) => {
    //             self.requests.push_back(Request::PingIndirect(header));
    //         }
    //         Request::PingIndirect(header) => {
    //             if let Some(member) = self.members.get(&header.member_id) {
    //                 // FIXME: it shouldn't be necessary to clone the member :/
    //                 let member = member.clone();
    //                 self.handle_suspect_other(&member);
    //             } else {
    //                 warn!(
    //                     self.logger,
    //                     "Trying to suspect a member that has already been removed: {}", header.member_id
    //                 );
    //             }
    //         }
    //         Request::PingProxy(request) => {
    //             warn!(
    //                 self.logger,
    //                 "Ping proxy from {} to {} timed out", request.sender.id, request.target.id
    //             );
    //         }
    //         _ => unreachable!(),
    //     }
    // }
    //
    // fn send_message(&mut self, target: SocketAddr, message: OutgoingMessage) {
    //     debug!(self.logger, "{:?} <- {:?}", target, message);
    //     match self.udp.as_ref().unwrap().send_to(message.buffer(), &target) {
    //         Err(e) => warn!(self.logger, "Message to {:?} was not delivered due to {:?}", target, e),
    //         Ok(count) => {
    //             debug!(self.logger, "Send {} bytes", count);
    //         }
    //     }
    // }
    //
    // fn recv_letter(&mut self) -> Option<IncomingLetter> {
    //     match self.udp.as_ref().unwrap().recv_from(&mut self.recv_buffer) {
    //         Ok((count, sender)) => {
    //             debug!(self.logger, "Received {} bytes from {:?}", count, sender);
    //             let message = match decode_message(&self.recv_buffer[..count]) {
    //                 Ok(message) => message,
    //                 Err(e) => {
    //                     warn!(self.logger, "Failed to decode from message {:#?}: {}", sender, e);
    //                     return None;
    //                 }
    //             };
    //             let letter = IncomingLetter { sender, message };
    //             debug!(self.logger, "{:?}", letter);
    //             Some(letter)
    //         }
    //         Err(e) => {
    //             warn!(self.logger, "Failed to receive letter due to {:?}", e);
    //             None
    //         }
    //     }
    // }
    //
    // fn update_members<'m>(&mut self, members: impl Iterator<Item = &'m Member>) {
    //     for member in members {
    //         self.update_member(member);
    //     }
    // }
    //
    // fn update_member(&mut self, member: &Member) {
    //     if member.id == self.myself.id {
    //         return;
    //     }
    //     // This can happen if this node is returning to a group before the group noticing that the node's previous
    //     // instance has died.
    //     if member.address == self.myself.address {
    //         warn!(self.logger, "Trying to add myself but with wrong ID {:?}", member);
    //         return;
    //     }
    //     match self.members.add_or_update(member.clone()) {
    //         Ok(_) => info!(self.logger, "Member joined: {:?}", member),
    //         Err(e) => warn!(self.logger, "Adding new member failed due to: {}", e),
    //     }
    // }
    //
    // fn process_notifications<'m>(&mut self, notifications: impl Iterator<Item = &'m Notification>) {
    //     for notification in notifications {
    //         if self.notifications.iter().find(|&n| n >= notification).is_some() {
    //             continue;
    //         }
    //         match notification {
    //             Notification::Confirm { member } => self.handle_confirm(member),
    //             Notification::Alive { member } => self.handle_alive(member),
    //             Notification::Suspect { member } => self.handle_suspect(member),
    //         }
    //         let obsolete_notifications = self
    //             .notifications
    //             .for_dissemination()
    //             .filter(|&n| n < notification)
    //             .cloned()
    //             .collect::<Vec<_>>();
    //         for n in obsolete_notifications {
    //             self.remove_notification(&n);
    //         }
    //         self.add_notification(notification.clone());
    //     }
    // }
    //
    // fn remove_notification(&mut self, notification: &Notification) {
    //     if notification.is_suspect() {
    //         self.remove_suspicion(notification.member());
    //     }
    //     self.notifications.remove_item(notification);
    // }
    //
    // fn add_notification(&mut self, notification: Notification) {
    //     // Suspect notification does not have a limit because it can be dropped only when it is moved to
    //     // Confirm or Alive. Suspect is limited by the respective Suspicion timeout.
    //     if notification.is_suspect() {
    //         self.notifications.add(notification);
    //     } else {
    //         self.notifications
    //             .add_with_limit(notification, self.config.notification_dissemination_times);
    //     }
    // }
    //
    // fn handle_confirm(&mut self, member: &Member) {
    //     self.remove_suspicion(member);
    //     match self.members.remove(&member.id) {
    //         Ok(_) => info!(self.logger, "Member {:?} removed", member),
    //         Err(e) => warn!(self.logger, "Member {:?} was not removed due to: {}", member, e),
    //     }
    // }
    //
    // fn remove_suspicion(&mut self, member: &Member) {
    //     if let Some(position) = self.suspicions.iter().position(|s| s.member == *member) {
    //         self.suspicions.remove(position);
    //     }
    // }
    //
    // fn handle_alive(&mut self, member: &Member) {
    //     self.remove_suspicion(member);
    //     self.update_member(member);
    // }
    //
    // fn handle_suspect(&mut self, member: &Member) {
    //     if member.id == self.myself.id {
    //         self.handle_suspect_myself(member);
    //     } else {
    //         self.handle_suspect_other(member);
    //         self.update_member(member);
    //     }
    // }
    //
    // fn handle_suspect_myself(&mut self, suspect: &Member) {
    //     if self.myself.incarnation <= suspect.incarnation {
    //         self.myself.incarnation = suspect.incarnation + 1;
    //         info!(
    //             self.logger,
    //             "I am being suspected, increasing my incarnation to {}", self.myself.incarnation
    //         );
    //         self.add_notification(Notification::Alive {
    //             member: self.myself.clone(),
    //         });
    //     }
    // }
    //
    // fn handle_suspect_other(&mut self, suspect: &Member) {
    //     // FIXME: Might be inefficient to check entire deq
    //     match self.suspicions.iter().position(|s| s.member.id == suspect.id) {
    //         Some(idx) if self.suspicions[idx].member.incarnation >= suspect.incarnation => {
    //             info!(
    //                 self.logger,
    //                 "Member {:?} is already suspected", self.suspicions[idx].member
    //             );
    //         }
    //         Some(idx) => {
    //             info!(
    //                 self.logger,
    //                 "Member {:?} suspected with lower incarnation, replacing it", self.suspicions[idx].member
    //             );
    //             self.suspicions.remove(idx);
    //             self.suspect_member(suspect)
    //         }
    //         None => self.suspect_member(suspect),
    //     }
    // }
    //
    // fn suspect_member(&mut self, suspect: &Member) {
    //     info!(self.logger, "Start suspecting member {:?}", suspect);
    //     let member = suspect.clone();
    //     self.suspicions.push_back(Suspicion::new(suspect.clone()));
    //     self.add_notification(Notification::Suspect { member });
    // }
    //
    // fn get_next_sequence_number(&mut self) -> u64 {
    //     let sequence_number = self.sequence_number;
    //     self.sequence_number += 1;
    //     sequence_number
    // }

    // fn update_state(&mut self, message: &DisseminationMessageIn) {
    //     self.update_members(std::iter::once(&message.sender).chain(message.broadcast.iter()));
    //     self.process_notifications(message.notifications.iter());
    // }
    //
    // fn handle_ack(&mut self, message: &DisseminationMessageIn) {
    //     for ack in self.acks.drain(..).collect::<Vec<_>>() {
    //         match ack.request {
    //             Request::Init(address) => {
    //                 self.update_state(message);
    //                 if message.sender.address != address || message.sequence_number != 0 {
    //                     panic!("Initial ping request failed, unable to continue");
    //                 }
    //                 continue;
    //             }
    //             Request::PingIndirect(ref header) => {
    //                 self.update_state(message);
    //                 if message.sender.id == header.member_id && message.sequence_number == header.sequence_number {
    //                     continue;
    //                 }
    //             }
    //             Request::PingProxy(ref ping_proxy) => {
    //                 if message.sender.id == ping_proxy.target.id
    //                     && message.sequence_number == ping_proxy.sequence_number
    //                 {
    //                     self.requests.push_back(Request::AckIndirect(AckIndirectRequest {
    //                         target: ping_proxy.sender.clone(),
    //                         sequence_number: ping_proxy.sequence_number,
    //                     }));
    //                     continue;
    //                 }
    //             }
    //             Request::Ping(ref header) => {
    //                 self.update_state(message);
    //                 if message.sender.id == header.member_id && message.sequence_number == header.sequence_number {
    //                     continue;
    //                 }
    //             }
    //             _ => unreachable!(),
    //         }
    //         self.acks.push(ack);
    //     }
    // }
    //
    // fn handle_ping(&mut self, message: &DisseminationMessageIn) {
    //     self.update_state(message);
    //     self.requests.push_back(Request::Ack(Header {
    //         member_id: message.sender.id,
    //         sequence_number: message.sequence_number,
    //     }));
    // }
    //
    // fn handle_indirect_ping(&mut self, message: &PingRequestMessageIn) {
    //     self.requests.push_back(Request::PingProxy(PingProxyRequest {
    //         sender: message.sender.clone(),
    //         target: message.target.clone(),
    //         sequence_number: message.sequence_number,
    //     }));
    // }
}

// HANDLE PROTOCOL EVENT WRITE
//             if let Some(request) = self.requests.pop_front() {
//                 debug!(self.logger, "{:?}", request);
//                 match request {
//                     Request::Init(address) => {
//                         let message = DisseminationMessageEncoder::new(1024)
//                             .message_type(MessageType::Ping)?
//                             .sender(&self.myself)?
//                             .sequence_number(0)?
//                             .encode();
//                         self.send_message(address, message);
//                         self.acks.push(Ack::new(request));
//                     }
//                     Request::Ping(ref header) if self.members.has(&header.member_id) => {
//                         let message = DisseminationMessageEncoder::new(1024)
//                             .message_type(MessageType::Ping)?
//                             .sender(&self.myself)?
//                             .sequence_number(header.sequence_number)?
//                             .notifications(self.notifications.for_dissemination())?
//                             .broadcast(self.members.for_broadcast())?
//                             .encode();
//                         self.send_message(self.members.get(&header.member_id).unwrap().address, message);
//                         self.acks.push(Ack::new(request));
//                     }
//                     Request::Ping(ref header) => {
//                         info!(
//                             self.logger,
//                             "Dropping Ping message, member {} has already been removed.", header.member_id
//                         );
//                     }
//                     Request::PingIndirect(ref header) if self.members.has(&header.member_id) => {
//                         let indirect_members = self
//                             .members
//                             .iter()
//                             .map(|m| m.id)
//                             .filter(|key| *key != header.member_id)
//                             .take(self.config.num_indirect as usize)
//                             .collect::<Vec<_>>();
//                         indirect_members.iter().try_for_each(|member_id| -> Result<()> {
//                             let message = PingRequestMessageEncoder::new()
//                                 .sender(&self.myself)?
//                                 .sequence_number(header.sequence_number)?
//                                 .target(&self.members.get(&header.member_id).unwrap())?
//                                 .encode();
//                             self.send_message(self.members.get(member_id).unwrap().address, message);
//                             Ok(())
//                         })?;
//                         self.acks.push(Ack::new(request));
//                     }
//                     Request::PingIndirect(ref header) => {
//                         info!(
//                             self.logger,
//                             "Dropping PingIndirect message, member {} has already been removed.", header.member_id
//                         );
//                     }
//                     Request::PingProxy(ref ping_proxy) => {
//                         let message = DisseminationMessageEncoder::new(1024)
//                             .message_type(MessageType::Ping)?
//                             .sender(&self.myself)?
//                             .sequence_number(ping_proxy.sequence_number)?
//                             .notifications(self.notifications.for_dissemination())?
//                             .broadcast(self.members.for_broadcast())?
//                             .encode();
//                         self.send_message(ping_proxy.target.address, message);
//                         self.acks.push(Ack::new(request));
//                     }
//                     Request::Ack(header) if self.members.has(&header.member_id) => {
//                         let message = DisseminationMessageEncoder::new(1024)
//                             .message_type(MessageType::PingAck)?
//                             .sender(&self.myself)?
//                             .sequence_number(header.sequence_number)?
//                             .notifications(self.notifications.for_dissemination())?
//                             .broadcast(self.members.for_broadcast())?
//                             .encode();
//                         self.send_message(self.members.get(&header.member_id).unwrap().address, message);
//                     }
//                     Request::Ack(header) => {
//                         warn!(
//                             self.logger,
//                             "Trying to send ACK {:?} to a member that has been removed", header
//                         );
//                     }
//                     Request::AckIndirect(ack_indirect) => {
//                         let message = DisseminationMessageEncoder::new(1024)
//                             .message_type(MessageType::PingAck)?
//                             .sender(&self.myself)?
//                             .sequence_number(ack_indirect.sequence_number)?
//                             .encode();
//                         self.send_message(ack_indirect.target.address, message);
//                     }
//                 }
//             }
