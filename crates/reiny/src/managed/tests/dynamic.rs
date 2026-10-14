use super::*;

#[tokio::test]
async fn dynamic_ports_verify_names_types_sources_and_fingerprints() {
    // Given declared byte-oriented ports on the real ordered bus.
    let bus = Arc::new(Local::new());
    let sender = producer(bus.clone(), SENDER).await;
    let receiver = consumer(bus.clone()).await;
    assert!(
        sender
            .raw_output("undefined", "test.Probe", Probe::SCHEMA_FINGERPRINT)
            .is_err()
    );
    assert!(
        sender
            .raw_input("outgoing", "test.Probe", Probe::SCHEMA_FINGERPRINT)
            .is_err()
    );
    assert!(
        receiver
            .raw_input("incoming", "other.Probe", Probe::SCHEMA_FINGERPRINT)
            .is_err()
    );
    let outgoing = sender
        .raw_output("outgoing", "test.Probe", Probe::SCHEMA_FINGERPRINT)
        .expect("output");
    let mut incoming = receiver
        .raw_input("incoming", "test.Probe", Probe::SCHEMA_FINGERPRINT)
        .expect("input");
    sender.ready().expect("sender ready");
    receiver.ready().expect("receiver ready");
    let invalid = bus
        .publisher(
            &Key::output(DOMAIN, SENDER, "outgoing", Probe::TYPE_NAME),
            &Qos::DEFAULT,
        )
        .expect("transport fixture");
    let foreign = bus
        .publisher(
            &Key::output(DOMAIN, "undeclared/source", "outgoing", Probe::TYPE_NAME),
            &Qos::DEFAULT,
        )
        .expect("foreign fixture");

    // When foreign, absent, malformed and mismatching stamps precede a valid sample.
    foreign
        .put(
            vec![99],
            Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec()),
        )
        .expect("foreign");
    for attachment in [None, Some(vec![1]), Some(0xffff_u64.to_le_bytes().to_vec())] {
        invalid.put(vec![98], attachment).expect("invalid stamp");
    }
    outgoing.send(vec![42]).expect("declared send");

    // Then only the exact source and schema reach the application.
    let sample = timeout(PATIENCE, incoming.recv())
        .await
        .expect("bounded receive")
        .expect("sample");
    assert_eq!(sample.payload, vec![42]);
    assert_eq!(sample.source, SENDER);
    assert_eq!(sample.schema, Some(Probe::SCHEMA_FINGERPRINT));
    drop(incoming);
    timeout(PATIENCE, receiver.shutdown())
        .await
        .expect("port loss requests shutdown");
}
