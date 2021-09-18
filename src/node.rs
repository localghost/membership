#![deny(missing_docs)]

use crate::result::Result;
use crate::sync_node::{ChannelMessage, SyncNode};
use crate::ProtocolConfig;
use anyhow::{anyhow, Context};
use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;

/// Runs the gossip protocol on an internal thread.
pub struct Node {
    bind_address: SocketAddr,
    config: Option<ProtocolConfig>,
    sender: Option<Sender<ChannelMessage>>,
    handle: Option<std::thread::JoinHandle<Result<()>>>,
    logger: Option<slog::Logger>,
}

impl Node {
    /// Creates new instance communicating with other members through `bind_address`.
    pub fn new(bind_address: SocketAddr, config: ProtocolConfig) -> Self {
        Node {
            bind_address,
            config: Some(config),
            sender: None,
            handle: None,
            logger: None,
        }
    }

    /// Set logger.
    pub fn set_logger(&mut self, logger: slog::Logger) {
        assert!(
            self.handle.is_none(),
            "Logger can only be set before starting the node."
        );
        self.logger = Some(logger);
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
        self.start()?;
        self.sender
            .as_ref()
            .unwrap()
            .blocking_send(ChannelMessage::Join(member))
            .with_context(|| format!("Failed to join member {}", member))
    }

    /// Starts new group.
    pub fn start(&mut self) -> Result<()> {
        assert!(self.handle.is_none(), "You have already started");

        let (mut sync_node, sender) = SyncNode::new(self.bind_address, self.config.take().unwrap());
        if let Some(logger) = self.logger.take() {
            sync_node.set_logger(logger)
        }
        self.sender = Some(sender);
        self.handle = Some(
            std::thread::Builder::new()
                .name("membership".to_string())
                .spawn(move || sync_node.start())?,
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
            .blocking_send(ChannelMessage::Stop)
            .with_context(|| format!("Failed to stop message"))?;
        self.wait()
    }

    /// Returns all alive members of the group this member knows about.
    ///
    /// These might not necessary be all alive members in the entire group.
    pub fn get_members(&self) -> Result<Vec<SocketAddr>> {
        assert!(self.handle.is_some(), "First you have to join");

        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .unwrap()
            .blocking_send(ChannelMessage::GetMembers(sender))
            .with_context(|| format!("Failed to ask for members"))?;
        receiver.recv().with_context(|| format!("Failed to get members"))
    }

    #[doc(hidden)]
    /// Waits for the member to finish.
    pub fn wait(&mut self) -> Result<()> {
        assert!(self.handle.is_some(), "You have not joined yet");
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|e| anyhow!("Membership thread panicked: {:?}", e))?
    }
}
