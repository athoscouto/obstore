# Copy

Copy operations.

Obstore provides [two API designs](../method-vs-functional-api.md) for your convenience.

## Method API

::: obstore.store.ObjectStoreMethods.copy
::: obstore.store.ObjectStoreMethods.copy_async

## Functional API

::: obstore.copy
::: obstore.copy_async

The functional and method APIs accept an optional `metadata` mapping for S3 copies.
When omitted (`None`), S3 copies source metadata and tags as usual. Supplying a mapping
replaces custom metadata on the destination; an empty mapping removes all custom
metadata. Keys are sent as `x-amz-meta-{key}`, including keys such as `content-type`;
that key is custom metadata and does not set the standard `Content-Type` header. S3's
`MetadataDirective=REPLACE` semantics also reset standard metadata headers such as
`Content-Type`, `Cache-Control`, and `Content-Disposition` unless the service supplies
defaults. Object tags remain copied from the source, matching S3's default tagging
directive. This matches an S3 CopyObject request with `MetadataDirective=REPLACE`.
This option is supported only for same-store S3 copies with `overwrite=True`.
