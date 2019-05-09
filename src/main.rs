use mio::*;
use mio::net::*;
use mio_extras::timer::*;
use std::io::Write;
use structopt::StructOpt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

#[derive(StructOpt, Default)]
struct Config {
    #[structopt(short="p", long="port", default_value="2345")]
    port: i32,

    #[structopt(short="b", long="bind-address", default_value="127.0.0.1")]
    bind_address: String
}

#[derive(Default)]
struct Gossip {
    config: Config,
    client: Option<UdpSocket>,
    server: Option<UdpSocket>,
    members: Vec<IpAddr>
}

impl Gossip {
    fn new(config: Config) -> Gossip {
        Gossip{config: config, ..Default::default()}
    }

    fn join(&mut self, _member: IpAddr) {
        self.members.push(_member);
//        let addr = format!("127.0.0.1:{}", self.config.port).parse().unwrap();
        let poll = Poll::new().unwrap();
        let mut timer = Builder::default().build::<()>();
        timer.set_timeout(Duration::from_secs(5), ());
        poll.register(&timer, Token(11), Ready::readable(), PollOpt::edge());

        self.bind(&poll);

        let mut events = Events::with_capacity(1024);
        let mut buffer: Vec<IpAddr>;
        loop {
            poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                println!("{:?}", event);
                if event.readiness().is_readable() {
                    match event.token() {
                        Token(43) => {
                            let mut buf = [0; 1024];
                            let (_, sender) = self.server.as_ref().unwrap().recv_from(&mut buf).unwrap();
                            println!("{:?}", String::from_utf8_lossy(&buf));
                            poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable() | Ready::readable(), PollOpt::edge()).unwrap();
                        }
                        Token(11) => {
                            buffer = self.members.iter().take(3).collect();
                            poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::writable() | Ready::readable(), PollOpt::edge()).unwrap();
                            timer.set_timeout(Duration::from_secs(5), ());
                        }
                        _ => unreachable!()
                    }
                } else if event.readiness().is_writable() {
//                    self.server.as_ref().unwrap().send_to()
                    self.server.as_ref().unwrap().send_to(b"ala ma kota", &SocketAddr::from_str("127.0.0.1:9876").unwrap());
                    poll.reregister(self.server.as_ref().unwrap(), Token(43), Ready::readable(), PollOpt::edge()).unwrap();
                }
            }
        }

    fn bind(&mut self, poll: &Poll) {
        let address = format!("{}:{}", self.config.bind_address, self.config.port).parse().unwrap();
        self.server = Some(UdpSocket::bind(&address).unwrap());
        poll.register(self.server.as_ref().unwrap(), Token(43), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    }

//    fn ping(&mut self, poll: &Poll) {
//        self.client = Some(TcpStream::connect(&addr).unwrap());
//        poll.register(self.client.as_ref().unwrap(), Token(42), Ready::writable(), PollOpt::edge()).unwrap();
//    }
}

fn main() {
    let config = Config::from_args();
    Gossip::new(config).join(IpAddr::from_str("127.0.0.1").unwrap());
}
