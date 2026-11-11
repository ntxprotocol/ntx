use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// The shared background runtime that all sources and receivers run on.
pub(crate) fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("ntx")
            .enable_all()
            .build()
            .expect("failed to start the NTX runtime")
    })
}
