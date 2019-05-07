use mio::*;
use mio::net::*;
use std::io::Write;

fn main() {
    let addr = "127.0.0.1:13265".parse().unwrap();
    let poll = Poll::new().unwrap();
    let mut client = TcpStream::connect(&addr).unwrap();
    poll.register(&client, Token(42), Ready::writable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(1024);
    loop {
        poll.poll(&mut events, None).unwrap();
//        for _event in events {
        client.write(b"dfdsfsd").unwrap();
//        }
    }
}
