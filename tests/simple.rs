//use futures::Future;
use getaddrs;
use libc;
use membership;
use tun_tap::Iface;

#[test]
fn simple_test() {
    //    use netsim::spawn;

    println!("{}", "OUTSIDE THREAD");
    for addr in getaddrs::InterfaceAddrs::query_system().unwrap() {
        println!("{:?}", addr.name);
    }

    std::thread::spawn(|| {
        println!("{}", "IN THREAD");
        unsafe {
            println!("{}", libc::unshare(libc::CLONE_NEWNET));
        }
        let iface = tun_tap::Iface::new("myiface", tun_tap::Mode::Tap).unwrap();
        for addr in getaddrs::InterfaceAddrs::query_system().unwrap() {
            println!("{:?}", addr.name);
        }
        std::thread::spawn(|| {
            println!("{}", "IN INNER THREAD");
            for addr in getaddrs::InterfaceAddrs::query_system().unwrap() {
                println!("{:?}", addr.name);
            }
        })
        .join();
    })
    .join();
    //    let spawn = spawn::new_namespace(|| {
    //        println!("{}", "IN NEW NAMESPACE");
    //        std::thread::spawn(|| {
    //        })
    //        .join();
    //    });
    //    spawn.wait();
}
