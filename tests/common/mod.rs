#![cfg(target_os = "linux")] // namespaces, iptables: all these is linux-specicfic.
use iptables;
use membership::{Node, ProtocolConfig};
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::Build;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

pub fn create_tun_interface(cidr: &str) {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let name = format!("tun{}", COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
    assert!(std::process::Command::new("ip")
        .args(&["tuntap", "add", "mode", "tun", &name])
        .output()
        .unwrap()
        .status
        .success());
    assert!(std::process::Command::new("ip")
        .args(&["address", "add", cidr, "dev", &name])
        .output()
        .unwrap()
        .status
        .success());
    assert!(std::process::Command::new("ip")
        .args(&["link", "set", &name, "up"])
        .output()
        .unwrap()
        .status
        .success());
    assert!(std::process::Command::new("ping")
        .args(&["-c1", "-W3", cidr.split('/').nth(0).unwrap()])
        .output()
        .unwrap()
        .status
        .success());
}

pub fn unshare_netns() {
    unsafe {
        if libc::unshare(libc::CLONE_NEWNET) == -1 {
            println!("Failed to unshare network namspace");
        }
    }
    assert!(std::process::Command::new("ip")
        .args(&["address", "add", "127.0.0.1/8", "dev", "lo"])
        .output()
        .unwrap()
        .status
        .success());
    assert!(std::process::Command::new("ip")
        .args(&["link", "set", "lo", "up"])
        .output()
        .unwrap()
        .status
        .success());
}

pub fn in_namespace<F>(code: F) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()> + Send + 'static,
{
    std::thread::spawn(|| -> anyhow::Result<()> {
        unshare_netns();

        code()
    })
    .join()
    .unwrap()
}

pub fn create_interfaces(num_interfaces: u8) -> Vec<String> {
    let addresses = (1..=num_interfaces)
        .map(|i| format!("192.168.0.{}", i))
        .collect::<Vec<String>>();
    addresses
        .iter()
        .for_each(|address| create_tun_interface(&format!("{}/24", address)));
    addresses
}

pub fn create_members(num_members: u8) -> Vec<Node> {
    let logger = TerminalLoggerBuilder::new()
        .level(sloggers::types::Severity::Debug)
        .build()
        .unwrap();

    let addresses = create_interfaces(num_members)
        .iter()
        .map(|a| SocketAddr::from_str(&format!("{}:2345", a)).unwrap())
        .collect::<Vec<_>>();
    let mut members = addresses
        .iter()
        .map(|a| Node::new(*a, Default::default()))
        .collect::<Vec<_>>();
    members.iter_mut().for_each(|m| m.set_logger(logger.clone()));
    members
}

// FIXME: doesn't work for now, a member that other member wants to join to may not have been started yet.
#[allow(dead_code)]
pub fn join_neighbours(members: &mut [Node]) -> anyhow::Result<()> {
    let join_addresses = members
        .iter()
        .skip(2)
        .chain(members.iter().take(1))
        .map(|m| m.bind_address())
        .collect::<Vec<_>>();
    members[0].start()?;
    members
        .iter_mut()
        .skip(1)
        .zip(join_addresses)
        .try_for_each(|(m, a)| m.join(a))
}

pub fn create_group(members: &mut [Node]) -> anyhow::Result<()> {
    let leader = &mut members[0];
    leader.start()?;

    let join_address = leader.bind_address();
    members.iter_mut().skip(1).try_for_each(|m| m.join(join_address))
}
//pub fn start_memberships(addresses: &Vec<String>) -> Vec<membership::Membership> {
//    let mut ms: Vec<membership::Membership> = Vec::new();
//    for (index, address) in addresses.iter().enumerate() {
//        let bind_address = SocketAddr::from_str(&format!("{}:2345", address)).unwrap();
//        let join_address = SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap();
//
//        let mut m = membership::Membership::new(bind_address, Default::default());
//        m.join(join_address).unwrap();
//
//        ms.push(m);
//    }
//    ms
//}

pub fn get_members_addresses(members: &[Node]) -> Vec<SocketAddr> {
    members.iter().map(|m| m.bind_address()).collect::<Vec<_>>()
}

pub fn assert_eq_unordered<T>(s1: &[T], s2: &[T])
where
    T: Eq + std::hash::Hash + std::fmt::Debug,
{
    assert_eq!(s1.iter().collect::<HashSet<_>>(), s2.iter().collect::<HashSet<_>>())
}

pub fn stop_members(mss: &mut [membership::Node]) -> anyhow::Result<()> {
    for ms in mss {
        ms.stop()?;
    }
    Ok(())
}

pub fn advance_epochs(num_epochs: u8) {
    std::thread::sleep(Duration::from_secs(
        ProtocolConfig::default().protocol_period * num_epochs as u64,
    ));
}

pub fn block_member(member: &Node) {
    slog::info!(logger(), "blocking member: {}", member.bind_address().ip().to_string());

    let ipt = iptables::new(false).unwrap();
    assert_eq!(
        ipt.append(
            "filter",
            "OUTPUT",
            &format!("--src {} -j DROP", member.bind_address().ip().to_string()),
        )
        .unwrap(),
        true
    );
}

pub fn unblock_member(member: &Node) {
    slog::info!(
        logger(),
        "unblocking member: {}",
        member.bind_address().ip().to_string()
    );

    let ipt = iptables::new(false).unwrap();
    assert_eq!(
        ipt.delete(
            "filter",
            "OUTPUT",
            &format!("--src {} -j DROP", member.bind_address().ip().to_string()),
        )
        .unwrap(),
        true
    );
}

fn logger() -> slog::Logger {
    TerminalLoggerBuilder::new()
        .level(sloggers::types::Severity::Info)
        .build()
        .unwrap()
}
