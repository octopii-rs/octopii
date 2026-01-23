use std::sync::Arc;
use tokio::runtime::{Builder, Handle, Runtime};

pub struct OctopiiRuntime {
    runtime: Option<Arc<Runtime>>,
    handle: Handle,
}

impl OctopiiRuntime {
    pub fn new(worker_threads: usize) -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(worker_threads * 2)
            .thread_name("octopii-worker")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let handle = runtime.handle().clone();

        Self {
            runtime: Some(Arc::new(runtime)),
            handle,
        }
    }

    pub fn from_handle(handle: Handle) -> Self {
        Self {
            runtime: None,
            handle,
        }
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

impl Default for OctopiiRuntime {
    fn default() -> Self {
        Self::new(4)
    }
}

impl Clone for OctopiiRuntime {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.as_ref().map(Arc::clone),
            handle: self.handle.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owned_runtime() {
        let runtime = OctopiiRuntime::new(2);

        let result = runtime.spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            42
        });

        let handle = runtime.handle();
        let output = handle.block_on(result).unwrap();
        assert_eq!(output, 42);
    }

    #[tokio::test]
    async fn test_from_handle() {
        let handle = tokio::runtime::Handle::current();
        let runtime = OctopiiRuntime::from_handle(handle);

        let result = runtime
            .spawn(async {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                "from_handle"
            })
            .await
            .unwrap();

        assert_eq!(result, "from_handle");
    }

    #[tokio::test]
    async fn test_clone() {
        let handle = tokio::runtime::Handle::current();
        let runtime1 = OctopiiRuntime::from_handle(handle);
        let runtime2 = runtime1.clone();

        let result1 = runtime1.spawn(async { 1 }).await.unwrap();
        let result2 = runtime2.spawn(async { 2 }).await.unwrap();

        assert_eq!(result1, 1);
        assert_eq!(result2, 2);
    }
}
