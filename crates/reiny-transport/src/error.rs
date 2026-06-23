use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("Protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("Channel send error")]
    ChannelSend,

    #[error("Zenoh error: {0}")]
    Zenoh(#[from] zenoh::Error),
}
