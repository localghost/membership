pub fn create_tun_interface(address: &str) {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let name = format!(
        "tun{}",
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    );
    std::process::Command::new("ip")
        .args(&["tuntap", "add", "mode", "tun", &name])
        .status()
        .unwrap();
    std::process::Command::new("ip")
        .args(&["address", "add", address, "dev", &name])
        .status()
        .unwrap();
    std::process::Command::new("ip")
        .args(&["link", "set", &name, "up"])
        .status()
        .unwrap();
    // TODO cut out mask from the address and ping
    //    std::process::Command::new("ping")
    //        .args(&["-c1", "-W3", address])
    //        .status()
    //        .unwrap();
}

pub fn unshare_netns() {
    unsafe {
        if libc::unshare(libc::CLONE_NEWNET) == -1 {
            println!("Failed to unshare network namspace");
        }
    }
    std::process::Command::new("ip")
        .args(&["address", "add", "127.0.0.1/8", "dev", "lo"])
        .status()
        .unwrap();
    std::process::Command::new("ip")
        .args(&["link", "set", "lo", "up"])
        .status()
        .unwrap();
}
