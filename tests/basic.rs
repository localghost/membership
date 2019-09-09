use membership;
use std::time::Duration;

mod common;

#[test]
fn all_members_alive() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = common::create_interfaces(3);
        let mut ms = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m
                .get_members()
                .iter()
                .map(|member| member.ip().to_string())
                .collect::<Vec<_>>();
            member_addresses.sort();
            assert_eq!(addresses, member_addresses);
        }
        ms.iter_mut().for_each(membership::Membership::stop);
    });
}

#[test]
fn dead_node_discovered() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = common::create_interfaces(3);
        let mut ms = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m
                .get_members()
                .iter()
                .map(|member| member.ip().to_string())
                .collect::<Vec<_>>();
            member_addresses.sort();
            assert_eq!(addresses, member_addresses);
        }

        ms.pop().unwrap().stop();

        std::thread::sleep(Duration::from_secs(10));

        for m in &ms {
            let mut member_addresses = m
                .get_members()
                .iter()
                .map(|member| member.ip().to_string())
                .collect::<Vec<_>>();
            member_addresses.sort();
            let expected_addresses = addresses.iter().rev().skip(1).rev().cloned().collect::<Vec<_>>();
            assert_eq!(expected_addresses, member_addresses);
        }

        ms.iter_mut().for_each(membership::Membership::stop);
    });
}
