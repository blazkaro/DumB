use std::io::ErrorKind;

pub trait RetryableError {
    fn is_retryable(&self) -> bool;
}

pub fn is_io_error_transient(err: &std::io::Error) -> bool {
    match err.kind() {
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut => true,
        _ => false,
    }
}
