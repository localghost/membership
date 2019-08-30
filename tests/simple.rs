use membership;

#[test]
fn simple_test() {
    use netsim::spawn;
    use tokio_core::reactor::Core;

    let spawn = spawn::new_namespace(|| {});
    let mut core = Core::new().unwrap();
    let interfaces = core.run(spawn_complete).unwrap();
    assert!(interfaces.is_empty());
}
