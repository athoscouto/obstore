# List

List operations.

S3 stores can opt into literal string prefixes with `S3Store(..., literal_prefix=True)`.
This setting affects recursive `list` and its `list_async` alias. `list_with_delimiter`
and `list_with_delimiter_async` keep their existing path-based prefix behavior.

Obstore provides [two API designs](../method-vs-functional-api.md) for your convenience.

## Method API

::: obstore.store.ObjectStoreMethods.list
::: obstore.store.ObjectStoreMethods.list_with_delimiter
::: obstore.store.ObjectStoreMethods.list_with_delimiter_async

## Functional API

::: obstore.list
::: obstore.list_with_delimiter
::: obstore.list_with_delimiter_async

## Types

::: obstore.ObjectMeta
::: obstore.ListResult
::: obstore.ListStream
::: obstore.ListChunkType
