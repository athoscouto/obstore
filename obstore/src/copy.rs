use std::collections::HashMap;

use object_store::{aws::S3CopyMetadata, CopyOptions};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{
    PyObjectStore, PyObjectStoreError, PyObjectStoreResult, PyPath, PyS3Store,
};

use crate::utils::PyNone;

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true, metadata=None))]
pub(crate) fn copy(
    py: Python,
    store: Bound<'_, PyAny>,
    from_: PyPath,
    to: PyPath,
    overwrite: bool,
    metadata: Option<Bound<'_, PyAny>>,
) -> PyObjectStoreResult<()> {
    let is_s3 = store.cast::<PyS3Store>().is_ok();
    let store: PyObjectStore = store.extract()?;
    let metadata = extract_metadata(metadata)?;
    let options = copy_options(is_s3, overwrite, metadata)?;
    let runtime = get_runtime();
    let from_ = from_.into();
    let to = to.into();
    py.detach(|| {
        runtime.block_on(store.as_ref().copy_opts(&from_, &to, options))?;
        Ok::<_, PyObjectStoreError>(())
    })
}

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true, metadata=None))]
pub(crate) fn copy_async<'py>(
    py: Python<'py>,
    store: Bound<'py, PyAny>,
    from_: PyPath,
    to: PyPath,
    overwrite: bool,
    metadata: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let is_s3 = store.cast::<PyS3Store>().is_ok();
    let store: PyObjectStore = store.extract()?;
    let metadata = extract_metadata(metadata)?;
    let options = copy_options(is_s3, overwrite, metadata)?;
    let from_ = from_.into();
    let to = to.into();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        store
            .as_ref()
            .copy_opts(&from_, &to, options)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyNone)
    })
}

fn extract_metadata(
    metadata: Option<Bound<'_, PyAny>>,
) -> PyResult<Option<HashMap<String, String>>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let mapping = PyDict::new(metadata.py());
    mapping.call_method1("update", (metadata,))?;
    mapping.extract().map(Some)
}

fn copy_options(
    is_s3: bool,
    overwrite: bool,
    metadata: Option<HashMap<String, String>>,
) -> PyResult<CopyOptions> {
    let mut options = CopyOptions::default();
    if let Some(metadata) = metadata {
        if !is_s3 {
            return Err(PyNotImplementedError::new_err(
                "replacement metadata is supported only for S3 stores",
            ));
        }
        if !overwrite {
            return Err(PyNotImplementedError::new_err(
                "replacement metadata cannot be combined with overwrite=False",
            ));
        }
        options.extensions.insert(S3CopyMetadata(metadata));
    }
    if !overwrite {
        options.mode = object_store::CopyMode::Create;
    }
    Ok(options)
}
