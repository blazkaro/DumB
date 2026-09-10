#[derive(Debug)]
pub enum FlushError {
    /// Could not open ss tables directory
    DirOpenFailed(std::io::Error),
    /// Flusher task is no longer running
    FlusherGone,
}
