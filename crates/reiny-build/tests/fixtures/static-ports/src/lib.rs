#![no_std]

mod generated {
    include!(concat!(env!("OUT_DIR"), "/reiny_generated.rs"));
}

pub use generated::*;
include!(concat!(env!("OUT_DIR"), "/reiny_ports.rs"));

pub fn declared_link() -> Result<reiny_link::Link<[u8; 512], 4>, reiny_link::Error> {
    let mut link = reiny_link::Link::new("fixture", PORTS, [0; 512], [0; 512])?;
    link.subscribes::<publications::State>("receive")?;
    link.publishes::<publications::State>("transmit")?;
    link.calls::<publications::Request>("calculate")?;
    link.serves::<publications::Request>("answer")?;
    Ok(link)
}

const _: () = {
    assert!(PORTS.len() == 4);
    assert!(PORTS[0].schema == <publications::State as reiny::Topic>::SCHEMA);
    assert!(matches!(PORTS[0].direction, reiny::PortDirection::Input));
    assert!(matches!(PORTS[1].direction, reiny::PortDirection::Output));
    assert!(matches!(PORTS[2].direction, reiny::PortDirection::Client));
    assert!(matches!(PORTS[3].direction, reiny::PortDirection::Service));
    match PORTS[2].response {
        Some(response) => {
            assert!(response.schema == <publications::Response as reiny::Topic>::SCHEMA);
        }
        None => panic!("RPC response missing"),
    }
};
