use membership;
use membership::ProtocolConfig;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

#[test]
fn simple_test() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));

    std::thread::spawn(|| {
        common::unshare_netns();
        common::create_tun_interface("192.168.0.1/24");
        common::create_tun_interface("192.168.0.2/24");
        let mut membership = membership::Membership::new(ProtocolConfig {
            bind_address: "192.168.0.2".to_string(),
            port: 2345,
            protocol_period: 5,
            ack_timeout: 1,
            num_indirect: 3,
        });

        let mut membership2 = membership::Membership::new(ProtocolConfig {
            bind_address: "192.168.0.1".to_string(),
            port: 2345,
            protocol_period: 5,
            ack_timeout: 1,
            num_indirect: 3,
        });
        membership.join(IpAddr::from_str("192.168.0.1").unwrap());
        membership2.join(IpAddr::from_str("192.168.0.2").unwrap());
        std::thread::sleep(Duration::from_secs(10));
        println!("ALIVE MEMBERS: {:?}", membership.get_members());
        membership.stop();
        membership2.stop();
    })
    .join()
    .unwrap();
}
