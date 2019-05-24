use mio::*;
use mio::net::*;
use mio_extras::timer::*;
use std::io::Write;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr, Ipv4Addr};
use std::str::FromStr;
use std::time::Duration;
use bytes::{BufMut, Buf, BytesMut};
use std::io::Cursor;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt;
use log::{debug};
use env_logger;
use std::convert::TryInto;
use std::default::Default;
use std::path::Prefix::DeviceNS;

#[derive(StructOpt, Default)]
struct ProtocolConfig {
    #[structopt(short="p", long="port", default_value="2345")]
    port: u16,

    #[structopt(short="b", long="bind-address", default_value="127.0.0.1")]
    bind_address: String,

    #[structopt(short="o", long="proto-period", default_value="5")]
    protocol_period: u64,

    #[structopt(short="a", long="ack-timeout", default_value="1")]
    ack_timeout: u8,
}

#[derive(StructOpt, Default)]
struct Config {
    #[structopt(short="j", long="join-address", default_value="127.0.0.1")]
    join_address: String,

    #[structopt(flatten)]
    proto_config: ProtocolConfig,
}

struct Gossip {
    config: ProtocolConfig,
    client: Option<UdpSocket>,
    server: Option<UdpSocket>,
    timer: Timer<()>,
    members: Vec<SocketAddr>,
    members_presence: HashSet<IpAddr>,
    next_member_index: usize,
    epoch: u64,
    recv_buffer: [u8; 64],
}

impl Default for Gossip {
    fn default() -> Self {
        Gossip {
           config: Default::default(),
            client: None,
            server: None,
            timer: Default::default(),
            members: Default::default(),
            members_presence: Default::default(),
            next_member_index: 0,
            epoch: 0,
            recv_buffer: [0; 64]
        }
    }
}

#[derive(Debug)]
enum MessageType {
    PING,
    PING_ACK,
}

struct Message {
    buffer: BytesMut,
}

impl Message {
    fn create(message_type: MessageType, sequence_number: u64, epoch: u64) -> Self {
        let mut message = Message{ buffer: BytesMut::with_capacity(64) };
        message.buffer.put_i32_be(message_type as i32);
        message.buffer.put_u64_be(sequence_number);
        message.buffer.put_u64_be(epoch);
        message
    }

    fn with_members(&mut self, members: &[SocketAddr]) -> usize {
        // 00101001 -> 5 addresses (highest 1 defines when the address types start): v4, v6, v4, v4, v6
        let mut cursor = Cursor::new(&mut self.buffer);
        let position = cursor.position();
        let count = std::cmp::min(members.len(), std::mem::size_of::<u8>()-1);
        let mut header = 0u8;
        for idx in 0..count {
            match members[idx].ip() {
                IpAddr::V4(ip) => {
                    cursor.get_mut().put_slice(&(ip.octets()));
                }
                IpAddr::V6(ip) => {
                    cursor.get_mut().put_u8(1);
//                    self.buffer.put_slice(&(ip.octets()));
                    header |= 1 << idx;
                }
            }
        }
        header |= 1 << count;
        cursor.set_position(position);
        cursor.get_mut().put_u8(header);
        count
    }

    fn get_type(&self) -> MessageType {
        let encoded_type = self.get_cursor_into_buffer(0).get_i32_be();
        match encoded_type {
            x if x == MessageType::PING as i32 => {
                MessageType::PING
            },
            x if x == MessageType::PING_ACK as i32 => {
                MessageType::PING_ACK
            },
            _ => {
                panic!("No such message type")
            }
        }
    }

    fn get_sequence_number(&self) -> u64 {
        self.get_cursor_into_buffer(std::mem::size_of::<i32>() as u64).get_u64_be()
    }

    fn get_epoch(&self) -> u64 {
        self.get_cursor_into_buffer(
            (std::mem::size_of::<i32>() + std::mem::size_of::<u64>()) as u64
        ).get_u64_be()
    }

    fn get_members(&self) -> Vec<SocketAddr> {
        let mut cursor = self.get_cursor_into_buffer(
            (std::mem::size_of::<i32>() + std::mem::size_of::<u64>() * 2) as u64
        );
        let header = cursor.get_u8();
        let mut result = Vec::with_capacity(count as usize);
        for idx in 0..count {
            result.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(cursor.get_u32_be())), 2345));
        }
        result
    }

    fn into_inner(self) -> BytesMut {
        self.buffer
    }

    fn get_cursor_into_buffer(&self, position: u64) -> Cursor<&BytesMut> {
        let mut cursor = Cursor::new(&self.buffer);
        cursor.set_position(position);
        cursor
    }
}

//impl<T: AsRef<[u8]>> From<T> for Message {
//    fn from(src: T) -> Self {
//        Message{ buffer: bytes::BytesMut::from(src.as_ref()) }
//    }
//}

impl From<&[u8; 64]> for  Message {
    fn from(src: &[u8; 64]) -> Self {
        Message{ buffer: bytes::BytesMut::from(&src[..]) }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Message {{ type: {:?}, epoch: {}, sequence_number: {}, members: {:?} }}",
               self.get_type(), self.get_epoch(), self.get_sequence_number(), self.get_members()
        )
    }
}

struct OutgoingLetter {
    target: SocketAddr,
    message: Message,
}

impl fmt::Debug for OutgoingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutgoingLetter {{ target: {}, message: {:?} }}", self.target, self.message)
    }
}

struct IncomingLetter {
    sender: SocketAddr,
    message: Message,
}

impl fmt::Debug for IncomingLetter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IncomingLetter {{ sender: {}, message: {:?} }}", self.sender, self.message)
    }
}

impl Gossip {
    fn new(config: ProtocolConfig) -> Gossip {
        Gossip{
            config: config,
            timer: Builder::default().build::<()>(),
            recv_buffer: [0; 64],
            ..Default::default()
        }
    }

    fn join(&mut self, _member: IpAddr) {
        self.members.push(SocketAddr::new(_member, self.config.port));
        self.members_presence.insert(_member);
        let poll = Poll::new().unwrap();
        poll.register(&self.timer, Token(11), Ready::readable(), PollOpt::edge());
        self.reset_protocol_timer();
        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut buffer: Vec<IpAddr>;
        let mut sequence_number: u64 = 0;
        let mut pings_to_confirm = VecDeque::with_capacity(1024);
        let mut wait_ack = HashMap::new();
        loop {
            poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                debug!("{:?}", event);
                if event.readiness().is_readable() {
                    match event.token() {
                        Token(53) => {
                            let letter = self.recv_letter();
                            self.members.append(letter.message.get_members().as_mut());
                            let m: HashSet<SocketAddr> = self.members.drain(..).collect();
                            self.members.extend(m.into_iter());
                            match letter.message.get_type() {
                                MessageType::PING => {
                                    if self.members_presence.insert(letter.sender.ip()) {
                                        self.members.push(letter.sender);
                                    }
                                    pings_to_confirm.push_back(letter);
                                    poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable()|Ready::writable(), PollOpt::edge()).unwrap();
                                }
                                MessageType::PING_ACK => {
                                    // check the key is in `wait_ack`, if it is not the node might have already be marked as failed
                                    // and removed from the cluster
                                    wait_ack.remove(&letter.message.get_sequence_number());
                                }
                                _ => unreachable!()
                            }
                        }
                        Token(11) => {
                            self.epoch += 1;
                            // TODO: drain `wait_ack` and mark the nodes as failed (suspected in the future).
                            poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable(), PollOpt::edge()).unwrap();
                            self.reset_protocol_timer();
                        }
                        _ => unreachable!()
                    }
                } else if event.readiness().is_writable() {
                    match event.token() {
                        Token(43) => {
                            let target = self.members[self.next_member_index];
                            let mut message = Message::create(MessageType::PING, sequence_number, self.epoch);
                            message.with_members(self.members.as_slice());
                            let result = self.send_letter(OutgoingLetter{message, target});
                            wait_ack.insert(sequence_number, target);
                            poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable()|Ready::writable(), PollOpt::edge()).unwrap();
                            sequence_number += 1;
                            // FIXME the members of this list will be added/removed during the list lifetime, thus don't assume
                            // that `next_member_index` is valid on subsequent iteration.
                            self.next_member_index = (self.next_member_index + 1) % self.members.len();
                        }
                        Token(53) => {
                            if let Some(confirm) = pings_to_confirm.pop_front() {
                                let mut message = Message::create(MessageType::PING_ACK, confirm.message.get_sequence_number(), confirm.message.get_epoch());
                                message.with_members(self.members.as_slice());
                                let letter = OutgoingLetter { message, target: confirm.sender };
                                self.send_letter(letter);
                            } else {
                                poll.reregister(self.server.as_ref().unwrap(), Token(53), Ready::readable(), PollOpt::edge()).unwrap();
                            }
                        }
                        _ => unreachable!()
                    }
                }
            }
        }
    }

    fn bind(&mut self, poll: &Poll) {
        let address = format!("{}:{}", self.config.bind_address, self.config.port).parse().unwrap();
        self.server = Some(UdpSocket::bind(&address).unwrap());
        poll.register(self.server.as_ref().unwrap(), Token(43), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    }

    fn reset_protocol_timer(&mut self) {
        self.timer.set_timeout(Duration::from_secs(self.config.protocol_period), ());
    }

    fn send_letter(&mut self, letter: OutgoingLetter) {
        debug!("{:?}", letter);
        self.server.as_ref().unwrap().send_to(&letter.message.into_inner(), &letter.target);
    }

    fn recv_letter(&mut self) -> IncomingLetter {
        let (_, sender) = self.server.as_ref().unwrap().recv_from(&mut self.recv_buffer).unwrap();
        let letter = IncomingLetter{sender, message: Message::from(&self.recv_buffer)};
        debug!("{:?}", letter);
        letter
    }
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Config::from_args();
//    let proto_config = ProtocolConfig::from_args();
    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
}
