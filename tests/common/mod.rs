use std::net::SocketAddr;
use std::str::FromStr;

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

pub fn in_namespace<F>(code: F)
where
    F: FnOnce() -> () + Send + 'static,
{
    std::thread::spawn(|| {
        unshare_netns();

        code();
    })
    .join()
    .unwrap();
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

pub fn start_memberships(addresses: &Vec<String>) -> Vec<membership::Membership> {
    let mut ms: Vec<membership::Membership> = Vec::new();
    for (index, address) in addresses.iter().enumerate() {
        let bind_address = SocketAddr::from_str(&format!("{}:2345", address)).unwrap();
        let join_address = SocketAddr::from_str(&format!("{}:2345", addresses[(index + 1) % addresses.len()])).unwrap();

        let mut m = membership::Membership::new(bind_address, Default::default());
        m.join(join_address);

        ms.push(m);
    }
    ms
}

pub fn get_member_addresses(ms: &membership::Membership) -> Vec<String> {
    let mut member_addresses = ms
        .get_members()
        .unwrap()
        .iter()
        .map(|member| member.ip().to_string())
        .collect::<Vec<_>>();
    member_addresses.sort();
    member_addresses
}
