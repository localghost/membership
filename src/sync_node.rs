#![deny(missing_docs)]

use crate::disseminated::Disseminated;
use crate::incoming_message::{DisseminationMessageIn, IncomingMessage, PingRequestMessageIn};
use crate::member::{Member, MemberId};
use crate::members::Members;
use crate::message::MessageType;
use crate::message_encoder::{DisseminationMessageEncoder, OutgoingMessage, PingRequestMessageEncoder};
use crate::messenger::{Messenger, MessengerImpl, OutgoingLetter};
use crate::notification::Notification;
use crate::result::Result;
use crate::suspicion::Suspicion;
use crate::ProtocolConfig;
use anyhow::Context;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime;
use tokio::time as tt;
use tracing::{debug, error, info, warn};

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
enum Request {
    // Join cluster.
    Init(SocketAddr),
    // Direct Ping to another node.
    Ping(Header),
    // Indirect ping of a node via another node.
    PingIndirect(Header),
    // Man-in-the-middle Ping - a Ping request from one node to another one this node needs to forward.
    PingProxy(PingProxyRequest),
    // Confirm Ping.
    Ack(Header),
}

#[derive(Debug)]
pub(crate) enum ChannelMessage {
    Stop,
    GetMembers(std::sync::mpsc::SyncSender<Vec<SocketAddr>>),
    Join(SocketAddr),
}

macro_rules! extract_enum {
    ($value:expr, $elem:path) => {
        match $value {
            $elem(ref inner) => inner,
            _ => panic!("Did not match!"),
        }
    };
}

fn create_interval(seconds: u64, behavior: tt::MissedTickBehavior) -> tt::Interval {
    let mut interval = tt::interval(Duration::from_secs(seconds));
    interval.set_missed_tick_behavior(behavior);
    interval
}

/// Runs the protocol on current thread, blocking it.
pub(crate) struct SyncNode {
    config: ProtocolConfig,
    notifications: Disseminated<Notification>,
    epoch: u64,
    sequence_number: u64,
    myself: Member,
    receiver: tokio::sync::mpsc::Receiver<ChannelMessage>,
    acks: Vec<Ack>,
    suspicions: VecDeque<Suspicion>,
    logger: slog::Logger,
    members: Members,
    msg_sender: Option<tokio::sync::mpsc::Sender<OutgoingLetter>>,
    msg_receiver: Option<tokio::sync::mpsc::Receiver<crate::messenger::IncomingLetter>>,
    requests_sender: tokio::sync::mpsc::UnboundedSender<Request>,
    requests_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
}

impl SyncNode {
    pub(crate) fn new(
        bind_address: SocketAddr,
        config: ProtocolConfig,
    ) -> (SyncNode, tokio::sync::mpsc::Sender<ChannelMessage>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (requests_sender, requests_receiver) = tokio::sync::mpsc::unbounded_channel();
        let gossip = SyncNode {
            config,
            notifications: Disseminated::new(),
            epoch: 0,
            sequence_number: 0,
            myself: Member::new(bind_address),
            receiver,
            acks: Vec::<Ack>::with_capacity(32),
            suspicions: VecDeque::new(),
            logger: slog::Logger::root(slog::Discard, slog::o!()),
            members: Members::new(),
            msg_sender: None,
            msg_receiver: None,
            requests_sender,
            requests_receiver,
        };
        (gossip, sender)
    }

    pub(crate) fn set_logger(&mut self, logger: slog::Logger) {
        self.logger = logger.new(slog::o!("id" => self.myself.id.to_string()));
    }

    pub(crate) fn start(&mut self) -> Result<()> {
        let id_value = tracing::field::debug(self.myself.id);
        let runtime = runtime::Builder::new_current_thread().enable_all().build().unwrap();
        tracing::info_span!("", id = id_value).in_scope(|| -> Result<()> {
            runtime.block_on(async {
                let mut messenger = MessengerImpl::new();
                let (sender, receiver) = messenger.start(self.myself.address)?;
                self.msg_sender = Some(sender);
                self.msg_receiver = Some(receiver);
                self.run().await?;
                Ok(())
            })
        })
    }

    async fn run(&mut self) -> Result<()> {
        debug!("Running the protocol with {:?}", self.config);
        let mut protocol_interval = create_interval(self.config.protocol_period, tt::MissedTickBehavior::Burst);
        let mut ack_interval = create_interval(self.config.ack_timeout as u64, tt::MissedTickBehavior::Skip);
        let mut suspicion_interval = create_interval(self.config.suspect_timeout, tt::MissedTickBehavior::Skip);
        loop {
            tokio::select!(
                _ = protocol_interval.tick() => {
                    self.advance_epoch().await?
                }
                _ = ack_interval.tick() => {
                    self.handle_acks().await
                }
                _ = suspicion_interval.tick() => {
                    self.drain_timeout_suspicions()
                        .into_iter()
                        .for_each(|s| self.handle_timeout_suspicion(&s));
                }
                Some(letter) = self.msg_receiver.as_mut().unwrap().recv() => {
                    match letter.message {
                        IncomingMessage::Ping(m) => self.handle_ping(&m).await?,
                        IncomingMessage::Ack(m) => self.handle_ack(&m).await?,
                        IncomingMessage::PingRequest(m) => self.handle_indirect_ping(&m).await?,
                    }
                }
                Some(request) = self.requests_receiver.recv() => {
                    if let Err(e) = self.handle_request(request).await {
                        warn!("Failed to handle request: {}", e);
                    }
                }
                Some(message) = self.receiver.recv() => {
                    match message {
                        ChannelMessage::Join(address) => {
                            info!("Received request to join {}", address);
                            let sequence_number = self.get_next_sequence_number();
                            let message = self.create_protocol_message(MessageType::Ping, sequence_number)?;
                            self.send_message(OutgoingLetter { to: address, message })
                                .await
                                .context(format!("Failed to contact initial member"))?;
                            self.acks.push(Ack::new(Request::Init(address)));
                        }
                        ChannelMessage::Stop => {
                            // TODO: stop the node
                            todo!()
                        }
                        ChannelMessage::GetMembers(sender) => {
                            let members = std::iter::once(&self.myself.address)
                                .chain(self.members.iter().map(|m| &m.address))
                                .cloned()
                                .collect::<Vec<_>>();
                            if let Err(e) = sender.send(members) {
                                warn!("Failed to send list of members: {:?}", e);
                            }
                        }
                    }
                }
            )
        }
    }

    async fn handle_request(&mut self, request: Request) -> Result<()> {
        match request {
            Request::Init(_) => self.handle_request_init(request).await?,
            Request::Ping(_) => self.handle_request_ping(request).await?,
            Request::PingIndirect(_) => self.handle_request_ping_indirect(request).await?,
            Request::PingProxy(_) => self.handle_request_ping_proxy(request).await?,
            Request::Ack(_) => self.handle_request_ack(request).await?,
        }
        Ok(())
    }

    async fn handle_request_ack(&mut self, request: Request) -> Result<()> {
        let header = extract_enum!(request, Request::Ack);
        match self.members.get(&header.member_id).cloned() {
            Some(member) => {
                let message = DisseminationMessageEncoder::new(1024)
                    .message_type(MessageType::PingAck)?
                    .sender(&self.myself)?
                    .sequence_number(header.sequence_number)?
                    .notifications(self.notifications.for_dissemination())?
                    .broadcast(self.members.for_broadcast())?
                    .encode();
                self.send_message(OutgoingLetter {
                    message,
                    to: member.address,
                })
                .await
                .context("Failed to send ACK")?;
            }
            None => {
                warn!("Trying to send ACK {:?} to a member that has been removed", header);
            }
        }
        Ok(())
    }

    async fn handle_request_ping_proxy(&mut self, request: Request) -> Result<()> {
        let ping_proxy = extract_enum!(request, Request::PingProxy);
        let message = DisseminationMessageEncoder::new(1024)
            .message_type(MessageType::Ping)?
            .sender(&self.myself)?
            .sequence_number(ping_proxy.sequence_number)?
            .notifications(self.notifications.for_dissemination())?
            .broadcast(self.members.for_broadcast())?
            .encode();
        self.send_message(OutgoingLetter {
            message,
            to: ping_proxy.target.address,
        })
        .await
        .context("Failed to send PingProxy")?;
        self.acks.push(Ack::new(request));
        Ok(())
    }

    async fn handle_request_ping_indirect(&mut self, request: Request) -> Result<()> {
        let header = extract_enum!(request, Request::PingIndirect);
        match self.members.get(&header.member_id).cloned() {
            Some(target) => {
                let indirect_members = self
                    .members
                    .iter()
                    .filter(|key| key.id != target.id)
                    .take(self.config.num_indirect as usize)
                    .cloned()
                    .collect::<Vec<_>>();
                for member in indirect_members {
                    let message = PingRequestMessageEncoder::new()
                        .sender(&self.myself)?
                        .sequence_number(self.get_next_sequence_number())?
                        .target(&target)?
                        .encode();
                    self.send_message(OutgoingLetter {
                        message,
                        to: member.address,
                    })
                    .await
                    .context("Failed to send PingRequest")?;
                }
            }
            None => debug!("Member has already been removed {:?}", header.member_id),
        }
        Ok(())
    }

    async fn handle_request_init(&mut self, request: Request) -> Result<()> {
        let address = extract_enum!(request, Request::Init);
        let sequence_number = self.get_next_sequence_number();
        let message = self.create_protocol_message(MessageType::Ping, sequence_number)?;
        self.send_message(OutgoingLetter { to: *address, message })
            .await
            .context("Failed to contact initial member")?;
        self.acks.push(Ack::new(request));
        Ok(())
    }

    async fn handle_request_ping(&mut self, request: Request) -> Result<()> {
        let header = extract_enum!(request, Request::Ping);
        if let Some(member) = self.members.get(&header.member_id).cloned() {
            let message = DisseminationMessageEncoder::new(1024)
                .message_type(MessageType::Ping)?
                .sender(&self.myself)?
                .sequence_number(header.sequence_number)?
                .notifications(self.notifications.for_dissemination())?
                .broadcast(self.members.for_broadcast())?
                .encode();
            self.send_message(OutgoingLetter {
                to: member.address,
                message,
            })
            .await
            .context("Failed to send Ping message")?;
            self.acks.push(Ack::new(request));
        } else {
            debug!("Member {} already removed.", header.member_id);
        }
        Ok(())
    }

    async fn handle_acks(&mut self) {
        debug!("Handling acks");
        let now = std::time::Instant::now();
        let ack_timeout = Duration::from_secs(self.config.ack_timeout as u64);
        let (handle, postpone): (Vec<_>, Vec<_>) = self
            .acks
            .drain(..)
            .partition(|ack| ack.request_time + ack_timeout <= now);
        self.acks = postpone;
        for ack in handle {
            match self.handle_timeout_ack(ack).await {
                Ok(()) => {}
                Err(e) => error!("Failed to handled timed out ACK: {}", e),
            }
        }
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

    fn handle_timeout_suspicion(&mut self, suspicion: &Suspicion) {
        debug!("Suspicion timed out: {:?}", suspicion.member);
        // Check if the `suspicion` is in notifications. Assume that if it is not then
        // the member has already been moved to a different state and this `suspicion` can be dropped.
        let position = self
            .notifications
            .for_dissemination()
            .position(|n| n.is_suspect() && *n.member() == suspicion.member);
        if let Some(position) = position {
            self.notifications.remove(position);
            self.notifications.add(Notification::Confirm {
                member: suspicion.member.clone(),
            });
            self.handle_confirm(&suspicion.member)
        } else {
            debug!("Member {} already removed.", suspicion.member.id);
        }
    }

    async fn advance_epoch(&mut self) -> Result<()> {
        self.epoch += 1;
        info!("New epoch: {}", self.epoch);
        if let Some(member) = self.members.next() {
            let member = member.clone();
            let sequence_number = self.get_next_sequence_number();
            let message = DisseminationMessageEncoder::new(1024)
                .message_type(MessageType::Ping)?
                .sender(&self.myself)?
                .sequence_number(sequence_number)?
                .notifications(self.notifications.for_dissemination())?
                .broadcast(self.members.for_broadcast())?
                .encode();
            self.send_message(OutgoingLetter {
                to: member.address,
                message,
            })
            .await
            .context(format!("Failed to send PING"))?;
            self.acks.push(Ack::new(Request::Ping(Header {
                member_id: member.id,
                sequence_number,
            })));
        }
        Ok(())
    }

    fn create_protocol_message(&mut self, mtype: MessageType, seqnum: u64) -> Result<OutgoingMessage> {
        Ok(DisseminationMessageEncoder::new(1024)
            .message_type(mtype)?
            .sender(&self.myself)?
            .sequence_number(seqnum)?
            .notifications(self.notifications.for_dissemination())?
            .broadcast(self.members.for_broadcast())?
            .encode())
    }

    async fn send_message(&mut self, letter: OutgoingLetter) -> Result<()> {
        self.msg_sender
            .as_mut()
            .unwrap()
            .send(letter)
            .await
            .context(format!("Failed to send message"))
    }

    async fn ping_indirect(&mut self, target: &Member) -> Result<()> {
        let indirect_members = self
            .members
            .iter()
            .filter(|key| key.id != target.id)
            .take(self.config.num_indirect as usize)
            .cloned()
            .collect::<Vec<_>>();
        for member in indirect_members {
            let message = PingRequestMessageEncoder::new()
                .sender(&self.myself)?
                .sequence_number(self.get_next_sequence_number())?
                .target(&target)?
                .encode();
            self.send_message(OutgoingLetter {
                message,
                to: member.address,
            })
            .await
            .context("Failed to send PingRequest")?;
        }
        Ok(())
    }

    async fn handle_ack(&mut self, message: &DisseminationMessageIn) -> Result<()> {
        for ack in self.acks.drain(..).collect::<Vec<_>>() {
            match ack.request {
                Request::Init(address) => {
                    self.update_state(message);
                    if message.sender.address != address || message.sequence_number != 0 {
                        panic!("Initial ping request failed, unable to continue");
                    }
                    continue;
                }
                Request::PingProxy(ref ping_proxy) => {
                    if message.sender.id == ping_proxy.target.id
                        && message.sequence_number == ping_proxy.sequence_number
                    {
                        self.requests_sender.send(Request::Ack(Header {
                            member_id: ping_proxy.sender.id,
                            sequence_number: ping_proxy.sequence_number,
                        }))?;
                        continue;
                    }
                }
                Request::Ping(ref header) | Request::PingIndirect(ref header) => {
                    self.update_state(message);
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

    fn retry_after(&mut self, request: Request, timeout: Duration) {
        let sender = self.requests_sender.clone();
        tokio::spawn(async move {
            debug!("Retrying request {:?} after {:?}", request, timeout);
            tokio::time::sleep(timeout).await;
            if let Err(e) = sender.send(request) {
                // FIXME: This should cause node to shutdown, some internal fatal error
                // must have happened if we could not send to unbounded channel.
                warn!("Failed to re-try request: {}", e);
            }
        });
    }

    async fn handle_timeout_ack(&mut self, ack: Ack) -> Result<()> {
        debug!("Handling Ack that timed out: {:?}", ack);
        match ack.request {
            Request::Init(address) => {
                info!("Failed to join {}", address);
                self.retry_after(Request::Init(address), Duration::from_secs(2));
            }
            Request::Ping(header) => {
                match self.members.get(&header.member_id) {
                    Some(target) => {
                        self.ping_indirect(&target.clone()).await?;
                        // self.requests.push_back(Request::PingIndirect(header));
                        // FIXME: There is no point in adding the Ack and waiting for it to timeout
                        // if there are no members that indirct ping could be sent to. In such case
                        // we could already start suspecting the node.
                        self.acks.push(Ack::new(Request::PingIndirect(header)));
                    }
                    None => info!("Target member {} not found", header.member_id),
                }
            }
            Request::PingIndirect(header) => {
                if let Some(member) = self.members.get(&header.member_id) {
                    // FIXME: it shouldn't be necessary to clone the member :/
                    let member = member.clone();
                    self.handle_suspect_other(&member);
                } else {
                    warn!(
                        "Trying to suspect a member that has already been removed: {}",
                        header.member_id
                    );
                }
            }
            Request::PingProxy(request) => {
                warn!(
                    "Ping proxy from {} to {} timed out",
                    request.sender.id, request.target.id
                );
            }
            _ => unreachable!(),
        }
        Ok(())
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
            warn!(
                "Trying to add myself but with wrong id: myself={}, other={}",
                self.myself.id, member.id
            );
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
                .iter()
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

    fn get_next_sequence_number(&mut self) -> u64 {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        sequence_number
    }
    //
    // fn handle_protocol_event(&mut self, event: &Event) -> Result<()> {
    //     if event.readiness().is_readable() {
    //         if let Some(letter) = self.recv_letter() {
    //             match letter.message {
    //                 IncomingMessage::Ping(m) => self.handle_ping(&m),
    //                 IncomingMessage::Ack(m) => self.handle_ack(&m),
    //                 IncomingMessage::PingRequest(m) => self.handle_indirect_ping(&m),
    //             }
    //         }
    //     } else if event.readiness().is_writable() {
    //         if let Some(request) = self.requests.pop_front() {
    //             debug!(self.logger, "{:?}", request);
    //             match request {
    //                 Request::Init(address) => {
    //                     let message = DisseminationMessageEncoder::new(1024)
    //                         .message_type(MessageType::Ping)?
    //                         .sender(&self.myself)?
    //                         .sequence_number(0)?
    //                         .encode();
    //                     self.send_message(address, message);
    //                     self.acks.push(Ack::new(request));
    //                 }
    //                 Request::Ping(ref header) if self.members.has(&header.member_id) => {
    //                     let message = DisseminationMessageEncoder::new(1024)
    //                         .message_type(MessageType::Ping)?
    //                         .sender(&self.myself)?
    //                         .sequence_number(header.sequence_number)?
    //                         .notifications(self.notifications.for_dissemination())?
    //                         .broadcast(self.members.for_broadcast())?
    //                         .encode();
    //                     self.send_message(self.members.get(&header.member_id).unwrap().address, message);
    //                     self.acks.push(Ack::new(request));
    //                 }
    //                 Request::Ping(ref header) => {
    //                     info!(
    //                         self.logger,
    //                         "Dropping Ping message, member {} has already been removed.", header.member_id
    //                     );
    //                 }
    //                 Request::PingIndirect(ref header) if self.members.has(&header.member_id) => {
    //                     let indirect_members = self
    //                         .members
    //                         .iter()
    //                         .map(|m| m.id)
    //                         .filter(|key| *key != header.member_id)
    //                         .take(self.config.num_indirect as usize)
    //                         .collect::<Vec<_>>();
    //                     indirect_members.iter().try_for_each(|member_id| -> Result<()> {
    //                         let message = PingRequestMessageEncoder::new()
    //                             .sender(&self.myself)?
    //                             .sequence_number(header.sequence_number)?
    //                             .target(&self.members.get(&header.member_id).unwrap())?
    //                             .encode();
    //                         self.send_message(self.members.get(member_id).unwrap().address, message);
    //                         Ok(())
    //                     })?;
    //                     self.acks.push(Ack::new(request));
    //                 }
    //                 Request::PingIndirect(ref header) => {
    //                     info!(
    //                         self.logger,
    //                         "Dropping PingIndirect message, member {} has already been removed.", header.member_id
    //                     );
    //                 }
    //                 Request::PingProxy(ref ping_proxy) => {
    //                     let message = DisseminationMessageEncoder::new(1024)
    //                         .message_type(MessageType::Ping)?
    //                         .sender(&self.myself)?
    //                         .sequence_number(ping_proxy.sequence_number)?
    //                         .notifications(self.notifications.for_dissemination())?
    //                         .broadcast(self.members.for_broadcast())?
    //                         .encode();
    //                     self.send_message(ping_proxy.target.address, message);
    //                     self.acks.push(Ack::new(request));
    //                 }
    //                 Request::Ack(header) if self.members.has(&header.member_id) => {
    //                     let message = DisseminationMessageEncoder::new(1024)
    //                         .message_type(MessageType::PingAck)?
    //                         .sender(&self.myself)?
    //                         .sequence_number(header.sequence_number)?
    //                         .notifications(self.notifications.for_dissemination())?
    //                         .broadcast(self.members.for_broadcast())?
    //                         .encode();
    //                     self.send_message(self.members.get(&header.member_id).unwrap().address, message);
    //                 }
    //                 Request::Ack(header) => {
    //                     warn!(
    //                         self.logger,
    //                         "Trying to send ACK {:?} to a member that has been removed", header
    //                     );
    //                 }
    //                 Request::AckIndirect(ack_indirect) => {
    //                     let message = DisseminationMessageEncoder::new(1024)
    //                         .message_type(MessageType::PingAck)?
    //                         .sender(&self.myself)?
    //                         .sequence_number(ack_indirect.sequence_number)?
    //                         .encode();
    //                     self.send_message(ack_indirect.target.address, message);
    //                 }
    //             }
    //         }
    //     }
    //     Ok(())
    // }
    //
    fn update_state(&mut self, message: &DisseminationMessageIn) {
        self.update_members(std::iter::once(&message.sender).chain(message.broadcast.iter()));
        self.process_notifications(message.notifications.iter());
    }

    async fn handle_ping(&mut self, message: &DisseminationMessageIn) -> Result<()> {
        self.update_state(message);
        let ack = self.create_protocol_message(MessageType::PingAck, message.sequence_number)?;
        self.send_message(OutgoingLetter {
            to: message.sender.address,
            message: ack,
        })
        .await
        .context(format!("Failed to send Ack for {:?}", message))
    }

    async fn handle_indirect_ping(&mut self, message: &PingRequestMessageIn) -> Result<()> {
        let ping_proxy = DisseminationMessageEncoder::new(1024)
            .message_type(MessageType::Ping)?
            .sender(&message.sender)?
            .sequence_number(message.sequence_number)?
            .notifications(self.notifications.for_dissemination())?
            .broadcast(self.members.for_broadcast())?
            .encode();
        self.send_message(OutgoingLetter {
            to: message.target.address,
            message: ping_proxy,
        })
        .await
        .context(format!("Failed to send PingProxy for {:?}", message))?;
        self.acks.push(Ack::new(Request::PingProxy(PingProxyRequest {
            sender: message.sender.clone(),
            target: message.target.clone(),
            sequence_number: message.sequence_number,
        })));
        Ok(())
    }
}
