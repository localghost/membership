use membership;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

#[test]
fn simple_test() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = vec!["192.168.0.1", "192.168.0.2", "192.168.0.3"];
        for address in &addresses {
            common::create_tun_interface(&format!("{}/24", address));
        }

        let mut ms: Vec<membership::Membership> = Vec::new();
        for (index, address) in addresses.iter().enumerate() {
            let mut m = membership::Membership::new(SocketAddr::from_str(&format!("{}:2345", address)).unwrap(), Default::default());
            m.join(SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap());
            ms.push(m);
        }

        std::thread::sleep(Duration::from_secs(10));

        let sort_fn = |l: &SocketAddr, r: &SocketAddr| l.ip().cmp(&r.ip());
        let mut expected_members = [
            SocketAddr::from_str("192.168.0.1:2345").unwrap(),
            SocketAddr::from_str("192.168.0.2:2345").unwrap(),
        ];
        expected_members.sort_by(sort_fn);
        let expected_memebers = expected_members;

        for m in &ms {
            let mut members = m.get_members();
            members.sort_by(sort_fn);
            assert_eq!(members, expected_members);
        }

        for m in &mut ms {
            m.stop();
        }
    });
}
