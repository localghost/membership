use membership;

mod common;

#[test]
fn simple_test() {
    std::thread::spawn(|| {
        common::unshare_netns();
        common::create_tun_interface("192.168.0.1/24");
        common::create_tun_interface("192.168.0.2/24");
        println!(
            "{:?}",
            std::process::Command::new("ping")
                .args(&["-c1", "-W1", "192.168.0.1"])
                .output()
                .unwrap()
        );
        println!(
            "{:?}",
            std::process::Command::new("ip")
                .args(&["address"])
                .output()
                .unwrap()
        );
    })
    .join()
    .unwrap();
}
