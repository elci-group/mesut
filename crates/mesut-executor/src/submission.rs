/// Traits for handling task results and submissions.
pub trait TaskHandle: Send + Sync {
    /// Block until the task completes.
    fn wait(&self) -> Result<Vec<u8>, String>;

    /// Wait with a timeout.
    fn wait_timeout(&self, _timeout: std::time::Duration) -> Result<Option<Vec<u8>>, String>;

    /// Check if task is done without blocking.
    fn is_done(&self) -> bool;
}
