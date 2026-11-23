use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::S3RequestMetrics;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// A shareable handle for cumulative S3 HTTP transport-attempt counts.
///
/// Pass one handle to any number of S3Store instances, then poll snapshot().
/// Counters include retries and transport failures, and do not represent billed
/// requests. Credentials and local presigning are excluded. Collection performs
/// no Python callbacks; only snapshot() acquires the GIL.
#[derive(Debug, Clone)]
#[pyclass(name = "S3RequestMetrics", frozen, from_py_object)]
pub struct PyS3RequestMetrics {
    pub(crate) inner: Arc<S3RequestMetrics>,
}

impl PartialEq for PyS3RequestMetrics {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[pymethods]
impl PyS3RequestMetrics {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(S3RequestMetrics::default()),
        }
    }

    /// Return cumulative counts for fixed API names, including zero counters.
    ///
    /// Does not reset counters or retain stores, URLs, keys, or credentials.
    fn snapshot(&self) -> HashMap<&'static str, u64> {
        self.inner.snapshot()
    }

    /// Pickling creates a fresh, process-local handle with zero counts.
    fn __getnewargs__<'py>(&self, py: Python<'py>) -> Bound<'py, PyTuple> {
        PyTuple::empty(py)
    }
}
