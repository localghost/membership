use crate::incoming_message::IncomingMessage;
use crate::message_decoder::decode_message;
use crate::message_encoder::OutgoingMessage;
use crate::result::Result;
use std::fmt;
use std::net::SocketAddr;
use tokio;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub(crate) trait Messenger {
    /// Returns:
    ///  * a `Sender` for passing message to be send. The transport may depend on the message type.
    ///  * a `Receiver` for receiving messages from other peers.
    fn start(&mut self, address: SocketAddr) -> Result<(Sender<OutgoingLetter>, Receiver<IncomingLetter>)>;

    fn stop(&mut self) -> Result<()>;
}

pub(crate) struct IncomingLetter {
    pub(crate) from: SocketAddr,
    pub(crate) message: IncomingMessage,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingLetter{{ sender: {:#?}, message: {:#?} }}",
            self.from, self.message
        )
    }
}

pub(crate) struct OutgoingLetter {
    pub(crate) to: SocketAddr,
    pub(crate) message: OutgoingMessage,
}

impl fmt::Debug for OutgoingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OutgoingLetter{{ receiver: {:#?}, message: {:#?} }}",
            self.to, self.message
        )
    }
}

pub(crate) struct MessengerImpl {
    handle: Option<JoinHandle<Result<()>>>,
    stop_sender: Option<mpsc::Sender<()>>,
}

struct MessangerContext {}

struct MessangerActor {
    stop_receiver: mpsc::Receiver<()>,
    udp: Option<UdpSocket>,
    address: SocketAddr,
    egress: Receiver<OutgoingLetter>,
    ingress: Sender<IncomingLetter>,
    recv_buffer: Vec<u8>,
}

impl MessangerActor {
    fn new(
        address: SocketAddr,
        egress: Receiver<OutgoingLetter>,
        ingress: Sender<IncomingLetter>,
    ) -> (Self, mpsc::Sender<()>) {
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        (
            Self {
                stop_receiver,
                udp: Option::None,
                address,
                egress,
                ingress,
                recv_buffer: vec![0u8; 1500],
            },
            stop_sender,
        )
    }

    async fn run(&mut self) -> Result<()> {
        self.udp = Some(UdpSocket::bind(self.address).await?);
        loop {
            tokio::select! {
                _ = self.stop_receiver.recv() => break,
                Some(message) = self.egress.recv() => self.send(message).await,
                result = self.udp.as_mut().unwrap().recv_from(&mut self.recv_buffer) => {
                    match result {
                        Ok((count, sender)) => {
                            debug!("Received {} bytes from {:?}", count, sender);
                            let message = match decode_message(&self.recv_buffer[..count]) {
                                Ok(message) => {
                                    let letter = IncomingLetter { from: sender, message };
                                    debug!("{:?}", letter);
                                    self.ingress.send(letter).await?;
                                },
                                Err(e) => {
                                    warn!("Failed to decode from message {:#?}: {}", sender, e);
                                }
                            };
                        }
                        Err(e) => {
                            warn!("Failed to receive letter due to {:?}", e);
                        }
                    }
                }
            };
        }
        Ok(())
    }

    async fn send(&mut self, letter: OutgoingLetter) {
        debug!("Sending message to {}", letter.to);
        self.udp
            .as_mut()
            .unwrap()
            .send_to(letter.message.buffer(), letter.to)
            .await;
    }
}

impl MessengerImpl {
    pub(crate) fn new() -> Self {
        Self {
            handle: None,
            stop_sender: None,
        }
    }
}

impl Messenger for MessengerImpl {
    fn start(&mut self, address: SocketAddr) -> Result<(Sender<OutgoingLetter>, Receiver<IncomingLetter>)> {
        let (out_sender, out_receiver) = mpsc::channel(1024);
        let (in_sender, in_receiver) = mpsc::channel(1024);
        let (mut actor, stop_sender) = MessangerActor::new(address, out_receiver, in_sender);
        self.stop_sender = Some(stop_sender);
        self.handle = Some(tokio::spawn(async move { actor.run().await }));
        Ok((out_sender, in_receiver))
    }

    fn stop(&mut self) -> Result<()> {
        unimplemented!()
    }
}