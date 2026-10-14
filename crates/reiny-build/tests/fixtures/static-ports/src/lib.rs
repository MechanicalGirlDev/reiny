#![no_std]

mod generated {
    include!(concat!(env!("OUT_DIR"), "/reiny_generated.rs"));
}

pub use generated::*;
include!(concat!(env!("OUT_DIR"), "/reiny_ports.rs"));

pub fn declared_link() -> Result<reiny_link::Link<[u8; 512], 4>, reiny_link::Error> {
    let mut link = reiny_link::Link::new("fixture", PORTS, [0; 512], [0; 512])?;
    link.subscribes::<exported_types::State>("receive")?;
    link.publishes::<exported_types::State>("transmit")?;
    link.rpc_client::<exported_types::Request>("calculate")?;
    link.rpc_server::<exported_types::Request>("answer")?;
    Ok(link)
}

const _: () = {
    assert!(PORTS.len() == 4);
    assert!(PORTS[0].schema == <exported_types::State as reiny::MessageType>::SCHEMA_FINGERPRINT);
    assert!(matches!(PORTS[0].direction, reiny::PortDirection::Input));
    assert!(matches!(PORTS[1].direction, reiny::PortDirection::Output));
    assert!(matches!(
        PORTS[2].direction,
        reiny::PortDirection::RpcClient
    ));
    assert!(matches!(
        PORTS[3].direction,
        reiny::PortDirection::RpcServer
    ));
    match PORTS[2].response {
        Some(response) => {
            assert!(
                response.schema
                    == <exported_types::Response as reiny::MessageType>::SCHEMA_FINGERPRINT
            );
        }
        None => panic!("RPC response missing"),
    }
};
