use super::*;
use crate::component::process::{
    codec,
    protocol::{
        Envelope, GraphGeneration, HostGeneration, HostId, OwnerGeneration, PreparedDigest,
        ProtocolVersion, Sequence, SessionIdentity, frame_header,
        step::{MAX_RETAINED_BYTES, MAX_VALUE_BYTES, WireSchema, WireValue},
    },
    step_wire::{export_type, import_schema},
};
use crate::{
    encoded::EncodedType,
    step::{StepInput, StepOutput, StepType, StepValue},
};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

fn report(index: usize) -> crate::Result<StepPreparedReport> {
    let ty = StepType::from_encoded_type(EncodedType::new(
        "quota.Values",
        crate::SCHEMA_FINGERPRINT_VERSION,
        1,
    )?)?;
    Ok(StepPreparedReport {
        instance: format!("node{index}"),
        schema: WireSchema {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::from([("out".into(), export_type(&ty))]),
        },
        contract: Vec::new(),
        outputs: BTreeMap::from([(
            "out".into(),
            WireValue {
                ty: export_type(&ty),
                // Valid repeated field-one varints; protobuf payloads remain opaque here.
                bytes: [8, 0].repeat(MAX_VALUE_BYTES / 2),
            },
        )]),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unused_prepared_outputs_cannot_exceed_aggregate_retention() -> crate::Result<()> {
    collect_at_limit(0).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn literals_are_charged_before_collecting_prepared_outputs() -> crate::Result<()> {
    collect_at_limit(2).await
}

async fn collect_at_limit(initial_bytes: usize) -> crate::Result<()> {
    let identity = SessionIdentity {
        owner_generation: OwnerGeneration(1),
        graph_generation: GraphGeneration(2),
        host_id: HostId("quota".into()),
        host_generation: HostGeneration(3),
        graph_digest: PreparedDigest("graph".into()),
        bundle_digest: PreparedDigest("bundle".into()),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let count = (MAX_RETAINED_BYTES - initial_bytes) / MAX_VALUE_BYTES + 1;
    let steps = (0..count)
        .map(|index| {
            let report = report(index)?;
            Ok((
                report.instance,
                StepAssignment {
                    schema: report.schema,
                    contract: report.contract,
                    seed_outputs: BTreeSet::from(["out".into()]),
                },
            ))
        })
        .collect::<crate::Result<_>>()?;
    let remote_identity = identity.clone();
    let producer = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        for index in 0..=count {
            let body = if index < count {
                HostMessage::Steps(Box::new(StepHostMessage::Report(report(index)?)))
            } else {
                HostMessage::PreparedComplete {}
            };
            let payload = codec::encode(&Envelope {
                version: ProtocolVersion,
                session: remote_identity.clone(),
                sequence: Sequence(u64::try_from(index)?),
                body,
            })?;
            stream.write_all(&frame_header(&payload)?).await?;
            stream.write_all(&payload).await?;
        }
        crate::Result::<()>::Ok(())
    });
    let budget = Duration::from_secs(60);
    let (stream, _) = timeout(budget, listener.accept()).await??;
    let mut channel = Channel {
        stream,
        identity,
        sent: 0,
        received: None,
        healthy: true,
    };
    let mut reports = CollectedReports::default();
    reports.budget.charge(initial_bytes)?;
    let result = collect_reports(
        &mut channel,
        &steps,
        &BTreeMap::new(),
        &mut reports,
        Instant::now() + budget,
    )
    .await;
    timeout(budget, producer).await???;
    assert!(
        channel.healthy,
        "quota rejection must follow a valid received frame"
    );
    assert!(
        result.is_err(),
        "unused prepared outputs still consume retention"
    );
    assert_eq!(reports.steps.len(), count - 1);
    Ok(())
}

#[test]
fn prepared_reports_and_resolved_bootstrap_share_the_retention_budget() -> crate::Result<()> {
    use crate::component::process::{ProcessStepPlan, ProcessStepSeed, ProcessStepSource};
    let reports: BTreeMap<_, _> = (0..MAX_RETAINED_BYTES / MAX_VALUE_BYTES)
        .map(|index| {
            let report = report(index)?;
            Ok((report.instance.clone(), report))
        })
        .collect::<crate::Result<_>>()?;
    let mut plan = ProcessStepPlan {
        nodes: reports
            .iter()
            .map(|(name, report)| Ok((name.clone(), import_schema(report.schema.clone())?)))
            .collect::<crate::Result<_>>()?,
        inputs: BTreeMap::new(),
    };
    assert!(
        plan.clone().resolve(&reports).is_ok(),
        "exact boundary without bootstrap"
    );
    let ty = plan.nodes["node0"].outputs["out"].clone();
    plan.nodes
        .get_mut("node0")
        .ok_or_else(|| anyhow::anyhow!("missing fixture node"))?
        .inputs
        .insert("previous".into(), ty.clone());
    for initial in [
        ProcessStepSeed::Literal(StepValue::from_encoded(ty, vec![8, 0])),
        ProcessStepSeed::PreparedOutput(StepOutput {
            instance: "node0".into(),
            port: "out".into(),
        }),
    ] {
        plan.inputs.insert(
            StepInput {
                instance: "node0".into(),
                port: "previous".into(),
            },
            ProcessStepSource::PreviousTick {
                source: StepOutput {
                    instance: "node0".into(),
                    port: "out".into(),
                },
                initial,
            },
        );
        plan.validate()?;
        assert!(
            plan.clone().resolve(&reports).is_err(),
            "bootstrap coexists with prepared reports"
        );
    }
    Ok(())
}
