# SignalDesk Export Worker

A least-privilege Python 3.11 Redis Streams worker that claims export jobs from the SignalDesk control API, fetches only API-authoritative tenant-scoped diagnostics, deterministically renders CSV/JSON, and writes confined artifacts to the synthetic MinIO fixture.

Run with `signaldesk-export-worker` or one bounded poll with `signaldesk-export-worker --once`.

The worker requires its dedicated service credential, a consumer-name label, an unauthenticated standalone-primary Redis URL, and explicit synthetic MinIO access/secret values. Each process appends a random incarnation suffix to the configured label before using it as a Redis consumer identity, so a restarted process can never renew or acknowledge an older incarnation's reclaimed delivery. Object storage is hard-confined to `http://minio:9000`, bucket `signaldesk-exports`, prefix `exports/`, and path-style addressing. Both HTTPX and botocore explicitly ignore ambient proxy variables.

## Required control API recovery contract

Crash recovery requires `GET /internal/exports/{job_id}`, authenticated only by the
export-worker service credential. Its `200` response must have exactly these fields:

```json
{
  "export_job_id": "uuid",
  "organization_id": "uuid",
  "requested_by_user_id": "uuid",
  "correlation_id": "uuid",
  "format": "csv | json",
  "expected_object_key": "exports/{organization_id}/{export_job_id}.{format}",
  "snapshot_id": "uuid",
  "status": "claimed | completed",
  "object_key": "derived-path-at-most-128-characters | null",
  "object_sha256": "lowercase-64-hex | null",
  "size_bytes": "integer-0-through-1073741824 | null"
}
```

For `claimed`, all three artifact fields must be null. For `completed`, all three
must be non-null and `object_key` must exactly equal `expected_object_key`. Return
`404` for an absent job and `409` when the job is not in a worker-recoverable state.
Both key fields are path-only (no URL, percent encoding, backslash, traversal, query,
or fragment), at most 128 characters, and must exactly equal
`exports/{organization_id}/{export_job_id}.{format}` using the IDs and format in the
same response. The endpoint accepts no body or query scope assertions. Claim and
recovery return the same immutable `snapshot_id`; every diagnostics request and page
binds that ID, and completion submits it for authoritative validation.

The worker carries one conservative whole-message monotonic deadline through claim,
recovery, diagnostics raw-body reads, rendering, object storage, completion, and
completion recovery. Redis ownership is atomically checked and renewed without
incrementing delivery generation between external phases and immediately before a
conditional object creation or control completion. Object creation uses
`If-None-Match: *`; a precondition race is accepted only after an exact HEAD match of
length, content type, and all user metadata.

## Required immutable canonical-object contract

A socket timeout or client cancellation cannot prove that an already accepted object
PUT did not complete server-side after the Redis lease expired. The control API's
immutable snapshot makes every retry render the same bytes, and the worker also makes
content stable at the sink before publishing the exact API object key. For each job
snapshot it atomically creates at most one internal object:

`exports/.canonical/{organization_id}/{export_job_id}/{snapshot_id}.{format}`

The canonical object uses `If-None-Match: *`. Its content type is the final content
type and its metadata has exactly `sha256`, `export-id`, `org-id`, `correlation-id`,
`snapshot-id`, and `canonical-version=1`. A losing writer HEADs and bounded-GETs the winner, verifies
the exact identity metadata, length, media type, and body digest, and then uses those
canonical bytes for the API-mandated final path. Consequently a delayed stale final
PUT is byte-for-byte identical to a rightful PUT and cannot poison completion.

The minimal object-store/IAM contract is: the worker principal alone may HEAD, GET,
and conditionally PUT both the exact final key and the canonical key above; wildcard
conditional creation must be atomic; canonical objects must never be overwritten or
deleted while a job can be retried or an earlier process can resume; and canonical
keys must not be exposed as downloadable completed artifacts. The control API
completion contract and exact final object path do not change. If the current bucket
policy or lifecycle permits only the final key or deletes canonical keys, deployment
remains blocked: no worker-only timeout increase can close that stale server-side PUT
window.
