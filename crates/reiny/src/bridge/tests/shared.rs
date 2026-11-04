use super::*;

#[tokio::test]
async fn shared_publication_crosses_named_raw_bridge_as_protobuf() {
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let mut source = bindings("native");
    output(&mut source, "state", contract("Probe"));
    let native = open(left.clone(), source).await;
    let state = native.output::<Probe>("state").expect("state");
    let mut destination = bindings("client");
    input(&mut destination, "state", "bridge", contract("Probe"));
    let client = open(right.clone(), destination).await;
    let mut incoming = client.input::<Probe>("state").expect("state input");
    let (_bridge_a, _bridge_b, _bridge) = bridge_contexts(&left, &right).await;
    native.ready().expect("native ready");
    client.ready().expect("client ready");
    let value = Arc::new(Probe { value: 43 });

    state
        .send_shared(Arc::clone(&value))
        .await
        .expect("shared send");
    let envelope = timeout(PATIENCE, incoming.recv_shared_envelope())
        .await
        .expect("bridge delivery")
        .expect("sample");

    assert_eq!(
        (
            envelope.source.as_str(),
            envelope.source_port.as_str(),
            envelope.value.value
        ),
        ("bridge", "state", 43)
    );
    assert!(
        !Arc::ptr_eq(&value, &envelope.value),
        "the bridge is byte-oriented"
    );
}
