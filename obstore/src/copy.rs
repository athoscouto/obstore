use std::collections::HashMap;
use std::sync::Arc;

use object_store::{
    aws::{AmazonS3, S3CopyMetadata},
    path::Path,
    CopyOptions, PutResult,
};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{
    MaybePrefixedStore, PyObjectStore, PyObjectStoreResult, PyPath, PyS3Store,
};

use crate::put::PyPutResult;

/// Keep ordinary copies generic; extended S3 copies use the destination transport.
enum PreparedCopy {
    Standard {
        store: PyObjectStore,
        from: Path,
        to: Path,
        options: CopyOptions,
    },
    S3 {
        source: Arc<MaybePrefixedStore<AmazonS3>>,
        destination: Arc<MaybePrefixedStore<AmazonS3>>,
        from: Path,
        to: Path,
        version: Option<String>,
        metadata: Option<S3CopyMetadata>,
        return_result: bool,
    },
}

impl PreparedCopy {
    #[allow(clippy::too_many_arguments)]
    fn new(
        store: Bound<'_, PyAny>,
        from: PyPath,
        to: PyPath,
        overwrite: bool,
        metadata: Option<Bound<'_, PyAny>>,
        destination_store: Option<Bound<'_, PyAny>>,
        source_version_id: Option<String>,
        return_result: bool,
    ) -> PyResult<Self> {
        let metadata = extract_metadata(metadata)?;
        if destination_store.is_some() || source_version_id.is_some() || return_result {
            if !overwrite {
                return Err(PyNotImplementedError::new_err(
                    "extended S3 copies require overwrite=True",
                ));
            }
            let source = store
                .extract::<PyS3Store>()
                .map_err(|_| PyNotImplementedError::new_err("extended copies require S3 stores"))?;
            let destination = match destination_store {
                Some(store) => store.extract::<PyS3Store>().map_err(|_| {
                    PyNotImplementedError::new_err("extended copies require S3 stores")
                })?,
                None => source.clone(),
            };
            destination
                .as_ref()
                .inner()
                .validate_copy_source(source.as_ref().inner())
                .map_err(pyo3_object_store::PyObjectStoreError::from)?;
            Ok(Self::S3 {
                source: source.into_inner(),
                destination: destination.into_inner(),
                from: from.into(),
                to: to.into(),
                version: source_version_id,
                metadata: metadata.map(S3CopyMetadata),
                return_result,
            })
        } else {
            let is_s3 = store.cast::<PyS3Store>().is_ok();
            Ok(Self::Standard {
                store: store.extract()?,
                from: from.into(),
                to: to.into(),
                options: copy_options(is_s3, overwrite, metadata)?,
            })
        }
    }

    async fn execute(self) -> PyObjectStoreResult<Option<PutResult>> {
        match self {
            Self::Standard {
                store,
                from,
                to,
                options,
            } => {
                store.as_ref().copy_opts(&from, &to, options).await?;
                Ok(None)
            }
            Self::S3 {
                source,
                destination,
                from,
                to,
                version,
                metadata,
                return_result,
            } => {
                let from = source.full_path(&from);
                let to = destination.full_path(&to);
                let result = destination
                    .inner()
                    .copy_from(
                        source.inner(),
                        &from,
                        &to,
                        version.as_deref(),
                        metadata.as_ref(),
                    )
                    .await?;
                Ok(return_result.then_some(result))
            }
        }
    }
}

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true, metadata=None, destination_store=None, source_version_id=None, return_result=false))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn copy(
    py: Python,
    store: Bound<'_, PyAny>,
    from_: PyPath,
    to: PyPath,
    overwrite: bool,
    metadata: Option<Bound<'_, PyAny>>,
    destination_store: Option<Bound<'_, PyAny>>,
    source_version_id: Option<String>,
    return_result: bool,
) -> PyObjectStoreResult<Option<PyPutResult>> {
    let copy = PreparedCopy::new(
        store,
        from_,
        to,
        overwrite,
        metadata,
        destination_store,
        source_version_id,
        return_result,
    )?;
    py.detach(|| {
        get_runtime()
            .block_on(copy.execute())
            .map(|result| result.map(PyPutResult::new))
    })
}

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true, metadata=None, destination_store=None, source_version_id=None, return_result=false))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn copy_async<'py>(
    py: Python<'py>,
    store: Bound<'py, PyAny>,
    from_: PyPath,
    to: PyPath,
    overwrite: bool,
    metadata: Option<Bound<'py, PyAny>>,
    destination_store: Option<Bound<'py, PyAny>>,
    source_version_id: Option<String>,
    return_result: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let copy = PreparedCopy::new(
        store,
        from_,
        to,
        overwrite,
        metadata,
        destination_store,
        source_version_id,
        return_result,
    )?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let result = copy.execute().await?;
        Ok(result.map(PyPutResult::new))
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
