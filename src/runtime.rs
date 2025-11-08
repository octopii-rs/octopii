use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

/// An isolated tokio runtime that runs on its own dedicated thread pool.
/// This prevents the runtime from interfering with other parts of your application.
pub struct OctopiiRuntime {
    runtime: Arc<Runtime>,
}

impl OctopiiRuntime {
    /// Creates a new isolated runtime with a specific number of worker threads.
    ///
    /// # Arguments
    /// * `worker_threads` - Number of worker threads for the runtime (default: 4)
    ///
    /// # Example
    /// ```
    /// use octopii::OctopiiRuntime;
    ///
    /// let runtime = OctopiiRuntime::new(4);
    /// ```
    pub fn new(worker_threads: usize) -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(worker_threads) // Limit blocking pool to match worker count
            .thread_name("octopii-worker")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        Self {
            runtime: Arc::new(runtime),
        }
    }

    /// Creates a new runtime with default settings (4 worker threads)
    pub fn default() -> Self {
        Self::new(4)
    }

    /// Spawn a future on the isolated runtime
    ///
    /// # Example
    /// ```
    /// use octopii::OctopiiRuntime;
    ///
    /// let runtime = OctopiiRuntime::new(4);
    /// runtime.spawn(async {
    ///     println!("Running on isolated runtime!");
    /// });
    /// ```
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }

    /// Block on a future using the isolated runtime
    ///
    /// # Example
    /// ```
    /// use octopii::OctopiiRuntime;
    ///
    /// let runtime = OctopiiRuntime::new(4);
    /// let result = runtime.block_on(async {
    ///     // Your async code here
    ///     42
    /// });
    /// ```
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.runtime.block_on(future)
    }

    /// Get a handle to the runtime for spawning tasks from other threads
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

impl Clone for OctopiiRuntime {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolated_runtime() {
        let runtime = OctopiiRuntime::new(2);

        let result = runtime.block_on(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            42
        });

        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn_on_runtime() {
        let runtime = OctopiiRuntime::new(2);

        let handle = runtime.spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            "done"
        });

        let result = runtime.block_on(handle).unwrap();
        assert_eq!(result, "done");
    }
}
