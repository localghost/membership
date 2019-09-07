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

//pub fn compare_unordered<T, F>(mut s1: &[T], mut s2: &[T], mut compare: F)
//where
//    F: FnMut(&T, &T) -> std::cmp::Ordering + Clone,
//{
//    s1.sort_by(compare.clone());
//    s2.sort_by(compare);
//
//    assert!(s1.cmp(s2));
//}
