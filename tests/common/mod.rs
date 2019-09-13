use crate::TestResult;
use membership::{Membership, ProtocolConfig};
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

pub fn in_namespace<F>(code: F) -> Result<(), failure::Error>
where
    F: FnOnce() -> Result<(), failure::Error> + Send + 'static,
{
    std::thread::spawn(|| -> Result<(), failure::Error> {
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

pub fn create_members(num_members: u8) -> Vec<Membership> {
    let addresses = create_interfaces(num_members)
        .iter()
        .map(|a| SocketAddr::from_str(&format!("{}:2345", a)).unwrap())
        .collect::<Vec<_>>();
    let members = addresses
        .iter()
        .map(|a| Membership::new(*a, Default::default()))
        .collect::<Vec<_>>();
    members
}

pub fn join_neighbours(members: &mut [Membership]) -> Result<(), failure::Error> {
    let join_addresses = members
        .iter()
        .skip(1)
        .chain(members.iter().take(1))
        .map(|m| m.bind_address())
        .collect::<Vec<_>>();
    members.iter_mut().zip(join_addresses).try_for_each(|(m, a)| m.join(a))
}

pub fn start_memberships(addresses: &Vec<String>) -> Vec<membership::Membership> {
    let mut ms: Vec<membership::Membership> = Vec::new();
    for (index, address) in addresses.iter().enumerate() {
        let bind_address = SocketAddr::from_str(&format!("{}:2345", address)).unwrap();
        let join_address = SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap();

        let mut m = membership::Membership::new(bind_address, Default::default());
        m.join(join_address).unwrap();

        ms.push(m);
    }
    ms
}

pub fn get_members_addresses(members: &[Membership]) -> Vec<SocketAddr> {
    members.iter().map(|m| m.bind_address()).collect::<Vec<_>>()
}

pub fn assert_eq_unordered<T>(s1: &[T], s2: &[T])
where
    T: Eq + std::hash::Hash + std::fmt::Debug,
{
    assert_eq!(s1.iter().collect::<HashSet<_>>(), s2.iter().collect::<HashSet<_>>())
}

pub fn stop_members(mss: &mut [membership::Membership]) -> Result<(), failure::Error> {
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
