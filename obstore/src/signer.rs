use core::time::Duration;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::{HeaderName, HeaderValue, Method};
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::signer::{SignedUrlOptions, Signer};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{
    MaybePrefixedStore, PyAzureStore, PyGCSStore, PyObjectStoreError, PyObjectStoreResult,
    PyS3Store, PyUrl,
};
use url::Url;

use crate::path::PyPaths;

#[derive(Debug)]
pub(crate) enum SignCapableStore {
    S3(Arc<MaybePrefixedStore<AmazonS3>>),
    Gcs(Arc<MaybePrefixedStore<GoogleCloudStorage>>),
    Azure(Arc<MaybePrefixedStore<MicrosoftAzure>>),
}

impl<'py> FromPyObject<'_, 'py> for SignCapableStore {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(store) = obj.cast::<PyS3Store>() {
            Ok(Self::S3(store.get().as_ref().clone()))
        } else if let Ok(store) = obj.cast::<PyGCSStore>() {
            Ok(Self::Gcs(store.get().as_ref().clone()))
        } else if let Ok(store) = obj.cast::<PyAzureStore>() {
            Ok(Self::Azure(store.get().as_ref().clone()))
        } else {
            let py = obj.py();
            // Check for object-store instance from other library
            let cls_name = obj
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                "AzureStore",
                "GCSStore",
                "HTTPStore",
                "LocalStore",
                "MemoryStore",
                "S3Store",
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

            Err(PyValueError::new_err(format!(
                "Expected an S3Store, GCSStore, or AzureStore instance, got {}",
                obj.repr()?
            )))
        }
    }
}

impl Signer for SignCapableStore {
    fn signed_url<'life0, 'life1, 'async_trait>(
        &'life0 self,
        method: Method,
        path: &'life1 Path,
        expires_in: Duration,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Url>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.signed_url(method, path, expires_in),
            Self::Gcs(inner) => inner.signed_url(method, path, expires_in),
            Self::Azure(inner) => inner.signed_url(method, path, expires_in),
        }
    }

    fn signed_url_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        method: Method,
        path: &'life1 Path,
        expires_in: Duration,
        options: &'life2 SignedUrlOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Url>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.signed_url_opts(method, path, expires_in, options),
            Self::Gcs(inner) => inner.signed_url_opts(method, path, expires_in, options),
            Self::Azure(inner) => inner.signed_url_opts(method, path, expires_in, options),
        }
    }

    fn signed_urls<'life0, 'life1, 'async_trait>(
        &'life0 self,
        method: Method,
        paths: &'life1 [Path],
        expires_in: Duration,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Vec<Url>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.signed_urls(method, paths, expires_in),
            Self::Gcs(inner) => inner.signed_urls(method, paths, expires_in),
            Self::Azure(inner) => inner.signed_urls(method, paths, expires_in),
        }
    }
}

#[derive(Default)]
pub(crate) struct PySignedUrlOptions(SignedUrlOptions);

impl<'py> FromPyObject<'_, 'py> for PySignedUrlOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let options = obj.cast::<pyo3::types::PyDict>()?;
        for (key, _) in options.iter() {
            let key = key.extract::<PyBackedStr>()?;
            if !["extra_query", "signed_headers"].contains(&key.as_ref()) {
                return Err(PyValueError::new_err(format!(
                    "Unknown signed URL option {key:?}; expected 'extra_query' or 'signed_headers'"
                )));
            }
        }
        let mut result = SignedUrlOptions::default();
        if let Some(query) = options.get_item("extra_query")? {
            result.extra_query = query.extract()?;
        }
        if let Some(headers) = options.get_item("signed_headers")? {
            let headers = headers.extract::<std::collections::HashMap<String, String>>()?;
            for (name, value) in headers {
                let name = HeaderName::from_bytes(name.as_bytes())
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                let value = HeaderValue::from_str(&value)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                result.signed_headers.append(name, value);
            }
        }
        Ok(Self(result))
    }
}

pub(crate) struct PyMethod(Method);

impl<'py> FromPyObject<'_, 'py> for PyMethod {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s = obj.extract::<PyBackedStr>()?;
        let method = match s.as_ref() {
            "GET" => Method::GET,
            "PUT" => Method::PUT,
            "POST" => Method::POST,
            "HEAD" => Method::HEAD,
            "PATCH" => Method::PATCH,
            "TRACE" => Method::TRACE,
            "DELETE" => Method::DELETE,
            "OPTIONS" => Method::OPTIONS,
            "CONNECT" => Method::CONNECT,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported HTTP method {other}"
                )))
            }
        };
        Ok(Self(method))
    }
}

#[derive(IntoPyObject)]
pub(crate) struct PyUrls(Vec<PyUrl>);

#[derive(IntoPyObject)]
pub(crate) enum PySignResult {
    One(PyUrl),
    Many(PyUrls),
}

#[pyfunction(signature = (store, method, paths, expires_in, *, options=None))]
pub(crate) fn sign(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
    options: Option<PySignedUrlOptions>,
) -> PyObjectStoreResult<PySignResult> {
    let runtime = get_runtime();
    let method = method.0;

    let options = options.unwrap_or_default().0;
    py.detach(|| match paths {
        PyPaths::One(path) => {
            let url =
                runtime.block_on(store.signed_url_opts(method, &path, expires_in, &options))?;
            Ok(PySignResult::One(PyUrl::new(url)))
        }
        PyPaths::Many(paths) => {
            let urls = if options.is_empty() {
                runtime.block_on(store.signed_urls(method, &paths, expires_in))?
            } else {
                paths
                    .iter()
                    .map(|path| {
                        runtime.block_on(store.signed_url_opts(
                            method.clone(),
                            path,
                            expires_in,
                            &options,
                        ))
                    })
                    .collect::<object_store::Result<Vec<_>>>()?
            };
            Ok(PySignResult::Many(PyUrls(
                urls.into_iter().map(PyUrl::new).collect(),
            )))
        }
    })
}

#[pyfunction(signature = (store, method, paths, expires_in, *, options=None))]
pub(crate) fn sign_async(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
    options: Option<PySignedUrlOptions>,
) -> PyResult<Bound<PyAny>> {
    let method = method.0;
    let options = options.unwrap_or_default().0;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        match paths {
            PyPaths::One(path) => {
                let url = store
                    .signed_url_opts(method, &path, expires_in, &options)
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
                Ok(PySignResult::One(PyUrl::new(url)))
            }
            PyPaths::Many(paths) => {
                if options.is_empty() {
                    let urls = store
                        .signed_urls(method, &paths, expires_in)
                        .await
                        .map_err(PyObjectStoreError::ObjectStoreError)?;
                    return Ok(PySignResult::Many(PyUrls(
                        urls.into_iter().map(PyUrl::new).collect(),
                    )));
                }
                let mut urls = Vec::with_capacity(paths.len());
                for path in &paths {
                    urls.push(
                        store
                            .signed_url_opts(method.clone(), path, expires_in, &options)
                            .await
                            .map_err(PyObjectStoreError::ObjectStoreError)?,
                    );
                }
                Ok(PySignResult::Many(PyUrls(
                    urls.into_iter().map(PyUrl::new).collect(),
                )))
            }
        }
    })
}
