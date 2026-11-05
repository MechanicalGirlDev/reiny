use super::{
    Bytes, Deserialize, Error, INVALID, Object, Result, StepSchema, Time, UNSUPPORTED, boundary,
    export, failure, insert, object, output, variant, wrong,
};
use reiny::step::{StepGraph, StepInput, StepOutput, StepRequest, StepSource, StepType};
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaDocument {
    version: u32,
    inputs: BTreeMap<String, Port>,
    outputs: BTreeMap<String, Port>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Port {
    Schema(u64),
    Asserted(AssertedPort),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertedPort {
    schema: u64,
    semantics: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Address {
    instance: String,
    port: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Route {
    External {
        instance: String,
        port: String,
    },
    SameTick {
        instance: String,
        port: String,
        source: Address,
    },
    PreviousTick {
        instance: String,
        port: String,
        source: Address,
        initial: u64,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphDocument {
    version: u32,
    nodes: BTreeMap<String, u64>,
    routes: Vec<Route>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    instance: String,
    port: String,
    value: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestDocument {
    version: u32,
    dt: Time,
    inputs: Vec<Input>,
}

fn version(version: u32) -> Result<()> {
    if version != 1 {
        return Err(Error::new(UNSUPPORTED, "unsupported step document version"));
    }
    Ok(())
}

fn schemas(handles: BTreeMap<String, Port>) -> Result<BTreeMap<String, StepType>> {
    handles
        .into_iter()
        .map(|(name, port)| {
            let (id, semantics) = match port {
                Port::Schema(id) => (id, BTreeMap::new()),
                Port::Asserted(port) => (port.schema, port.semantics),
            };
            let value = object(id)?;
            let ty =
                StepType::from_encoded_type(variant!(value, Schema).clone()).map_err(failure)?;
            Ok((name, ty.with_semantics(semantics).map_err(failure)?))
        })
        .collect()
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    reiny::component::process::decode_payload(bytes)
        .map_err(|error| Error::new(INVALID, error.to_string()))
}

export!(reiny_step_schema_import(json: Bytes, out: *mut u64) {
    output(out)?;
    let document: SchemaDocument = decode(&json.bytes()?)?;
    version(document.version)?;
    let schema = StepSchema { inputs: schemas(document.inputs)?, outputs: schemas(document.outputs)? };
    out.write(insert(Object::StepSchema(schema), false)?);
    Ok(())
});

export!(reiny_step_graph_import(json: Bytes, out: *mut u64) {
    output(out)?;
    let document: GraphDocument = decode(&json.bytes()?)?;
    version(document.version)?;
    let mut graph = StepGraph { nodes: BTreeMap::new(), inputs: BTreeMap::new() };
    for (name, id) in document.nodes {
        let schema = object(id)?;
        graph.nodes.insert(name, variant!(schema, StepSchema).clone());
    }
    for route in document.routes {
        let (instance, port, source) = match route {
            Route::External { instance, port } => (instance, port, StepSource::External),
            Route::SameTick { instance, port, source } => (instance, port, StepSource::SameTick(StepOutput { instance: source.instance, port: source.port })),
            Route::PreviousTick { instance, port, source, initial } => {
                let initial = object(initial)?;
                (instance, port, StepSource::PreviousTick {
                    source: StepOutput { instance: source.instance, port: source.port },
                    initial: variant!(initial, StepValue).clone(),
                })
            }
        };
        if graph.inputs.insert(StepInput { instance, port }, source).is_some() {
            return Err(Error::new(INVALID, "duplicate step route"));
        }
    }
    out.write(insert(Object::StepGraph(graph), false)?);
    Ok(())
});

export!(reiny_step_request_import(json: Bytes, out: *mut u64) {
    output(out)?;
    let document: RequestDocument = decode(&json.bytes()?)?;
    version(document.version)?;
    let mut request = StepRequest { dt: document.dt.duration()?, inputs: BTreeMap::new() };
    for input in document.inputs {
        let value = object(input.value)?;
        if request.inputs.insert(StepInput { instance: input.instance, port: input.port }, variant!(value, StepValue).clone()).is_some() {
            return Err(Error::new(INVALID, "duplicate external input"));
        }
    }
    out.write(insert(Object::StepRequest(request), false)?);
    Ok(())
});
