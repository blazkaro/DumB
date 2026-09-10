use glommio_ng::{GlommioError, ResourceType};

#[derive(Debug)]
pub enum ClassifiedError<T = ()> {
    Io(std::io::Error),
    ChannelClosed(T),
    WouldBlock(T),
    TimedOut,
    Other(String),
}

pub fn classify_glommio_error<T>(e: GlommioError<T>) -> ClassifiedError<T> {
    match e {
        GlommioError::IoError(source) => ClassifiedError::Io(source),
        GlommioError::EnhancedIoError { source, .. } => ClassifiedError::Io(source),
        GlommioError::Closed(ResourceType::Channel(item)) => ClassifiedError::ChannelClosed(item),
        GlommioError::WouldBlock(ResourceType::Channel(item)) => ClassifiedError::WouldBlock(item),
        GlommioError::TimedOut(_) => ClassifiedError::TimedOut,
        other => ClassifiedError::Other(other.to_string()),
    }
}
