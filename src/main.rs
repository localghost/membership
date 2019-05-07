use mio::*;
use mio::net::*;
use std::io::Write;
use structopt::StructOpt;

#[derive(StructOpt, Default)]
struct Config {
    #[structopt(short="p", long="port", default_value="2345")]
    port: i32
}

#[derive(Default)]
struct Gossip {
    config: Config,
    client: Option<TcpStream>,
    server: Option<TcpListener>,
}

impl Gossip {
    fn new(config: Config) -> Gossip {
        Gossip{config: config, ..Default::default()}
    }

    fn connect(&mut self) {
        let addr = format!("127.0.0.1:{}", self.config.port).parse().unwrap();
        let poll = Poll::new().unwrap();
        self.server = Some(TcpListener::bind(&addr).unwrap());
        poll.register(self.server.as_ref().unwrap(), Token(43), Ready::readable(), PollOpt::edge()).unwrap();
        self.client = Some(TcpStream::connect(&addr).unwrap());
        poll.register(self.client.as_ref().unwrap(), Token(42), Ready::writable(), PollOpt::edge()).unwrap();
        let mut events = Events::with_capacity(1024);
        loop {
            poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                match event.token() {
                    Token(42) => {
                        self.client.as_mut().unwrap().write(b"dfdsfsd").unwrap();
                    }
                    Token(43) => {
                        self.server.as_ref().unwrap().accept();
                    }
                    _ => unreachable!()
                }
            }
        }
    }
}

fn main() {
    let config = Config::from_args();
    Gossip::new(config).connect();
}
