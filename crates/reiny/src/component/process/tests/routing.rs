use std::collections::BTreeMap;
use std::sync::Arc;

use super::super::{ProcessLimits, preparation};
use super::component::Behavior;
use super::fixture::{BUDGET, Event, Fixture, bindings, registration};
use super::protocol::*;
use crate::engine::{Key, Payload};

fn request(placement: BTreeMap<String, String>) -> Prepare {
    Prepare {
        assignments: vec![ComponentAssignment {
            implementation: registration(),
            bindings: bindings(),
            config_origin: None,
        }],
        transport: TransportSettings::RoutedZenoh {
            domain: "process-routing".into(),
            connect: Vec::new(),
            listen: Vec::new(),
            multicast_scouting: false,
            placement,
        },
        remaining_budget: Milliseconds(5000),
    }
}

fn placement() -> BTreeMap<String, String> {
    BTreeMap::from([("test/service".into(), "fixture".into())])
}

#[tokio::test]
async fn placement_rejection_precedes_factory_construction() -> crate::Result<()> {
    let mut oversized = placement();
    for index in 0..8 {
        oversized.insert(format!("test/remote{index}"), "remote".into());
    }
    let mut unknown_source = request(placement());
    unknown_source.assignments[0].bindings.inputs.insert(
        "input".into(),
        crate::bindings::InputBinding {
            contract: crate::bindings::EndpointContract {
                type_name: "Probe".into(),
                ..Default::default()
            },
            sources: vec![crate::bindings::EndpointAddress {
                namespace: "test/missing".into(),
                port: "data".into(),
            }],
        },
    );
    for request in [
        BTreeMap::new(),
        BTreeMap::from([("test/service".into(), "other".into())]),
        BTreeMap::from([
            ("test/service".into(), "fixture".into()),
            ("test/extra".into(), "fixture".into()),
        ]),
        BTreeMap::from([
            ("test/service".into(), "fixture".into()),
            ("test/*".into(), "other".into()),
        ]),
        BTreeMap::from([
            ("test/service".into(), "fixture".into()),
            ("test/remote".into(), "bad/host".into()),
        ]),
        oversized,
    ]
    .into_iter()
    .map(request)
    .chain([unknown_source])
    {
        let mut fixture = Fixture::start(Behavior::Idle).await?;
        fixture.authenticate().await?;
        fixture.send(OwnerMessage::Prepare(request)).await?;
        assert!(matches!(
            fixture.outcome().await?,
            HostOutcome::Failed { primary, .. } if primary.code == FailureCode::Assignment
        ));
        fixture.join()?;
        assert_eq!(
            fixture
                .constructed
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(fixture.events.try_recv().is_err());
    }
    Ok(())
}

#[tokio::test]
async fn routed_host_commits_runs_and_stops() -> crate::Result<()> {
    let mut fixture = Fixture::start(Behavior::Idle).await?;
    fixture.authenticate().await?;
    fixture
        .send(OwnerMessage::Prepare(request(placement())))
        .await?;
    assert!(matches!(fixture.receive().await?, HostMessage::Report(_)));
    assert_eq!(
        fixture
            .constructed
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.receive().await?, HostMessage::PreparedComplete {});
    fixture.send(OwnerMessage::Commit {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Committed {});
    fixture.send(OwnerMessage::Run {}).await?;
    assert_eq!(fixture.receive().await?, HostMessage::Running {});
    assert!(matches!(fixture.event().await?, Event::Prepare(_, _)));
    assert_eq!(fixture.event().await?, Event::Run);
    fixture
        .send(OwnerMessage::Stop {
            reason: StopReason::Requested,
            remaining_budget: Milliseconds(5000),
        })
        .await?;
    assert_eq!(fixture.outcome().await?, HostOutcome::Stopped {});
    assert_eq!(fixture.join()?, HostOutcome::Stopped {});
    assert_eq!(fixture.event().await?, Event::Stop);
    Ok(())
}

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    sequence: u32,
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_host_engine_preserves_native_local_payload_identity() -> crate::Result<()> {
    let (options, _) = preparation::prepare(
        request(placement()),
        &[registration()],
        ProcessLimits::default(),
        "fixture",
    )
    .await?;
    let key = Key::output("process-routing", "test/service", "data", "Probe");
    let (sender, receiver) = flume::unbounded();
    let guard = options.engine.subscribe_delivery(
        &key,
        Box::new(move |sample| {
            assert!(sender.send(sample).is_ok(), "live receiver");
        }),
    )?;
    let publisher = options.engine.publisher(&key, &crate::Qos::DEFAULT)?;
    let payload = Arc::new(Probe { sequence: 42 });
    publisher.put_payload(Payload::shared(Arc::clone(&payload)), Some(vec![17; 8]))?;
    let sample = tokio::time::timeout(BUDGET, receiver.recv_async()).await??;
    assert_eq!(sample.key, key);
    assert_eq!(sample.attachment, Some(vec![17; 8]));
    assert!(Arc::ptr_eq(
        &sample
            .payload
            .downcast::<Probe>()
            .ok_or_else(|| anyhow::anyhow!("native payload"))?,
        &payload
    ));
    // The mirrored Zenoh publication still encodes. Arc reuse proves local
    // delivery only, not a no-serialization send path.
    drop((guard, publisher));
    Ok(())
}

#[test]
fn legacy_decoder_rejects_new_variant_without_transport_fallback() -> crate::Result<()> {
    // The exact pre-extension transport shape. No negotiated capability exists.
    #[derive(serde::Deserialize)]
    #[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
    enum Legacy {
        Zenoh {
            domain: String,
            connect: Vec<String>,
            listen: Vec<String>,
            multicast_scouting: bool,
        },
    }
    let bytes = super::codec::encode(&request(placement()).transport)?;
    assert!(super::codec::decode::<Legacy>(&bytes).is_err());
    let bytes = super::codec::encode(&TransportSettings::Local {
        domain: "local".into(),
        placement: placement(),
    })?;
    assert!(super::codec::decode::<Legacy>(&bytes).is_err());
    let bytes = super::codec::encode(&TransportSettings::Zenoh {
        domain: "legacy".into(),
        connect: vec![],
        listen: vec![],
        multicast_scouting: false,
    })?;
    let Legacy::Zenoh {
        domain,
        connect,
        listen,
        multicast_scouting,
    } = super::codec::decode(&bytes)?;
    assert_eq!(domain, "legacy");
    assert!(connect.is_empty() && listen.is_empty() && !multicast_scouting);
    Ok(())
}
