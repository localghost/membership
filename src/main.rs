use mio::*;
use mio::net::*;
use mio_extras::timer::*;
use std::io::Write;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;
use bytes::{BufMut, Buf, BytesMut};
use std::io::Cursor;

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

#[derive(Default)]
struct Gossip {
    config: ProtocolConfig,
    client: Option<UdpSocket>,
    server: Option<UdpSocket>,
    timer: Timer<()>,
    members: Vec<IpAddr>,
    next_member_index: usize,
    epoch: u64,
}

#[derive(Debug)]
enum MessageType {
    PING,
    PING_ACK,
}

struct Message {
    buffer: BytesMut
}

impl Message {
    fn create(message_type: MessageType, sequence_number: u64, epoch: u64) -> Self {
        let mut message = Message{ buffer: BytesMut::with_capacity(32) };
        message.buffer.put_i32_be(message_type as i32);
        message.buffer.put_u64_be(sequence_number);
        message.buffer.put_u64_be(epoch);
        message
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

    fn into_inner(self) -> BytesMut {
        self.buffer
    }

    fn get_cursor_into_buffer(&self, position: u64) -> Cursor<&BytesMut> {
        let mut cursor = Cursor::new(&self.buffer);
        cursor.set_position(position);
        cursor
    }
}

impl<T: AsRef<[u8]>> From<T> for Message {
    fn from(src: T) -> Self {
        Message{ buffer: bytes::BytesMut::from(src.as_ref()) }
    }
}

impl Gossip {
    fn new(config: ProtocolConfig) -> Gossip {
        Gossip{
            config: config,
            timer: Builder::default().build::<()>(),
            ..Default::default()
        }
    }

    fn join(&mut self, _member: IpAddr) {
        self.members.push(_member);
//        let addr = format!("127.0.0.1:{}", self.config.port).parse().unwrap();
        let poll = Poll::new().unwrap();
        poll.register(&self.timer, Token(11), Ready::readable(), PollOpt::edge());
        self.reset_protocol_timer();
        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut buffer: Vec<IpAddr>;
        let mut sequence_number: u64 = 0;
        loop {
            poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                println!("{:?}", event);
                if event.readiness().is_readable() {
                    match event.token() {
                        Token(43) => {
                            let mut buf = [0; 32];
                            let (_, sender) = self.server.as_ref().unwrap().recv_from(&mut buf).unwrap();
                            let message = Message::from(&buf);
//                            let mut message = Cursor::new(buf);
                            println!("From {}: {:?} -> {}", sender.to_string(), message.get_type(), message.get_sequence_number());
                            poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable() | Ready::readable(), PollOpt::edge()).unwrap();
                        }
                        Token(11) => {
//                            buffer = self.members.iter().take(3).collect();
                            poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable() | Ready::readable(), PollOpt::edge()).unwrap();
                            self.epoch += 1;
                            self.reset_protocol_timer()
                        }
                        _ => unreachable!()
                    }
                } else if event.readiness().is_writable() {
                    let message = Message::create(MessageType::PING, sequence_number, self.epoch);
                    let result = self.server.as_ref().unwrap().send_to(&message.into_inner(), &SocketAddr::new(self.members[self.next_member_index], self.config.port));
                    println!("SEND RESULT: {:?}", result);
                    poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::readable(), PollOpt::edge()).unwrap();
                    sequence_number += 1;
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

//    fn ping(&mut self, poll: &Poll) {
//        self.client = Some(TcpStream::connect(&addr).unwrap());
//        poll.register(self.client.as_ref().unwrap(), Token(42), Ready::writable(), PollOpt::edge()).unwrap();
//    }
}

fn main() {
    let config = Config::from_args();
//    let proto_config = ProtocolConfig::from_args();
    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
}
