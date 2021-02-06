use crate::incoming_message::IncomingMessage;
use crate::message_encoder::OutgoingMessage;
use crate::result::Result;
use std::fmt;
use std::net::SocketAddr;
use tokio;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

pub(crate) trait Messenger {
    /// Returns:
    ///  * a `Sender` for passing message to be send. The transport may depend on the message type.
    ///  * a `Receiver` for receiving messages from other peers.
    fn start(&mut self, address: SocketAddr) -> Result<(Sender<OutgoingLetter>, Receiver<IncomingLetter>)>;

    fn stop(&mut self) -> Result<()>;
}

pub(crate) struct IncomingLetter {
    pub(crate) sender: SocketAddr,
    pub(crate) message: IncomingMessage,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingLetter{{ sender: {:#?}, message: {:#?} }}",
            self.sender, self.message
        )
    }
}

pub(crate) struct OutgoingLetter {
    pub(crate) receiver: SocketAddr,
    pub(crate) message: OutgoingMessage,
}

impl fmt::Debug for OutgoingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OutgoingLetter{{ receiver: {:#?}, message: {:#?} }}",
            self.receiver, self.message
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
            };
        }
        Ok(())
    }

    async fn send(&mut self, letter: OutgoingLetter) {
        self.udp
            .as_mut()
            .unwrap()
            .send_to(letter.message.buffer(), letter.receiver)
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
