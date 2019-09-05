use membership;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

#[test]
fn simple_test() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    std::thread::spawn(|| {
        common::unshare_netns();
        common::create_tun_interface("192.168.0.1/24");
        common::create_tun_interface("192.168.0.2/24");

        let mut m1 = membership::Membership::new(SocketAddr::from_str("192.168.0.1:2345").unwrap(), Default::default());
        let mut m2 = membership::Membership::new(SocketAddr::from_str("192.168.0.2:2345").unwrap(), Default::default());
        m1.join(SocketAddr::from_str("192.168.0.2:2345").unwrap());
        m2.join(SocketAddr::from_str("192.168.0.1:2345").unwrap());

        std::thread::sleep(Duration::from_secs(10));

        let sort_fn = |l: &SocketAddr, r: &SocketAddr| l.ip().cmp(&r.ip());
        let mut expected_members = [
            SocketAddr::from_str("192.168.0.1:2345").unwrap(),
            SocketAddr::from_str("192.168.0.2:2345").unwrap(),
        ];
        assert_eq!(m1.get_members().sort_by(sort_fn), expected_members.sort_by(sort_fn));
        assert_eq!(m2.get_members().sort_by(sort_fn), expected_members.sort_by(sort_fn));

        m1.stop();
        m2.stop();
    })
    .join()
    .unwrap();
}
