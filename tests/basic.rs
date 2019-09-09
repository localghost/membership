use membership;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

#[test]
fn all_members_alive() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = vec!["192.168.0.1", "192.168.0.2", "192.168.0.3"];

        let mut ms: Vec<membership::Membership> = Vec::new();
        for (index, address) in addresses.iter().enumerate() {
            common::create_tun_interface(&format!("{}/24", address));

            let bind_address = SocketAddr::from_str(&format!("{}:2345", address)).unwrap();
            let join_address = SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap();

            let mut m = membership::Membership::new(bind_address, Default::default());
            m.join(join_address);

            ms.push(m);
        }

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m.get_members().iter().map(|member| member.ip().to_string()).collect::<Vec<_>>();
            member_addresses.sort();
            assert_eq!(addresses, member_addresses);
        }

        for m in &mut ms {
            m.stop();
        }
    });
}

#[test]
fn dead_node_discovery() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = vec!["192.168.0.1", "192.168.0.2", "192.168.0.3"];

        let mut ms: Vec<membership::Membership> = Vec::new();
        for (index, address) in addresses.iter().enumerate() {
            common::create_tun_interface(&format!("{}/24", address));

            let bind_address = SocketAddr::from_str(&format!("{}:2345", address)).unwrap();
            let join_address = SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap();

            let mut m = membership::Membership::new(bind_address, Default::default());
            m.join(join_address);

            ms.push(m);
        }

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m.get_members().iter().map(|member| member.ip().to_string()).collect::<Vec<_>>();
            member_addresses.sort();
            assert_eq!(addresses, member_addresses);
        }

        ms.pop().unwrap().stop();

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m.get_members().iter().map(|member| member.ip().to_string()).collect::<Vec<_>>();
            member_addresses.sort();
            let expected_addresses = addresses.iter().rev().skip(1).rev().cloned().collect::<Vec<_>>();
            assert_eq!(expected_addresses, member_addresses);
        }

        for m in &mut ms {
            m.stop();
        }
    });
}
