# Sign

::: obstore.sign
::: obstore.sign_async
::: obstore.SignCapableStore
::: obstore.HTTP_METHOD
::: obstore.SignedUrlOptions

Use `options` to bind additional query parameters or request headers into the signature.
For example, an S3 upload can require a specific content type and a checksum header:

```python
url = obstore.sign(
    store,
    "PUT",
    "reports/result.csv",
    timedelta(hours=1),
    options={
        "signed_headers": {
            "content-type": "text/csv",
            "x-amz-checksum-sha256": checksum,
        },
    },
)
```

The request using the returned URL must include those headers with the same values.
Use `extra_query` for parameters such as `versionId`, `response-content-type`, or
`response-content-disposition`. `extra_query` is a sequence of `(name, value)` pairs,
so repeated query keys can be represented. Stores that do not support signing these
options return their normal object store `NotSupported` error.
