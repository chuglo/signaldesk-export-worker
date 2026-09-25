from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import traceback
from uuid import UUID, uuid4

import httpx
import pytest
import signaldesk_export_worker.control_client as control_client_module

from signaldesk_export_worker.control_client import (
    ClaimConflict,
    ControlApiClient,
    FatalCredentialError,
    TransientControlError,
)

CREDENTIAL = "export-worker-service-credential-0001"


def client(handler: httpx.BaseTransport, *, cap: int = 16384) -> ControlApiClient:
    return ControlApiClient(
        base_url="https://control:8443",
        credential=CREDENTIAL,
        timeout=1,
        max_response_bytes=cap,
        transport=handler,
    )


def scope(job_id: UUID, *, status: str = "claimed") -> dict[str, object]:
    organization_id = uuid4()
    return {
        "export_job_id": str(job_id),
        "organization_id": str(organization_id),
        "requested_by_user_id": str(uuid4()),
        "correlation_id": str(uuid4()),
        "format": "csv",
        "expected_object_key": f"exports/{organization_id}/{job_id}.csv",
        "snapshot_id": str(uuid4()),
        "status": status,
        "object_key": None,
        "object_sha256": None,
        "size_bytes": None,
    }


def test_claim_uses_dedicated_credential_and_strict_identity_response() -> None:
    job_id = uuid4()
    payload = scope(job_id)
    for key in ("status", "object_key", "object_sha256", "size_bytes"):
        payload.pop(key)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=payload)

    result = client(httpx.MockTransport(handler)).claim(job_id)
    assert result.export_job_id == job_id
    assert seen[0].headers["accept-encoding"] == "identity"
    assert seen[0].headers["x-signaldesk-service-credential"] == CREDENTIAL
    assert seen[0].url.path == f"/internal/exports/{job_id}/claim"


def test_claim_requires_snapshot_id() -> None:
    job_id = uuid4()
    payload = scope(job_id)
    for key in ("status", "object_key", "object_sha256", "size_bytes", "snapshot_id"):
        payload.pop(key)
    with pytest.raises(TransientControlError):
        client(
            httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
        ).claim(job_id)


def test_control_client_ignores_hostile_proxy_environment(monkeypatch) -> None:
    job_id = uuid4()
    payload = scope(job_id)
    for key in ("status", "object_key", "object_sha256", "size_bytes"):
        payload.pop(key)
    encoded = json.dumps(payload).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    try:
        direct = ControlApiClient(
            base_url=f"http://127.0.0.1:{server.server_port}",
            credential=CREDENTIAL,
            timeout=0.2,
        )
        try:
            assert direct.claim(job_id).export_job_id == job_id
        finally:
            direct.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_fetch_anticipated_recovery_contract_is_strict() -> None:
    job_id = uuid4()
    payload = scope(job_id, status="completed")
    payload["object_key"] = payload["expected_object_key"]
    payload["object_sha256"] = "a" * 64
    payload["size_bytes"] = 123
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    result = client(transport).fetch(job_id)
    assert result.status == "completed"
    assert result.object_sha256 == "a" * 64

    bad = payload | {"tenant_override": str(uuid4())}
    with pytest.raises(TransientControlError):
        client(
            httpx.MockTransport(lambda request: httpx.Response(200, json=bad))
        ).fetch(job_id)

    bad_path = payload | {
        "expected_object_key": "exports/../escape.csv",
        "object_key": "exports/../escape.csv",
    }
    with pytest.raises(TransientControlError):
        client(
            httpx.MockTransport(lambda request: httpx.Response(200, json=bad_path))
        ).fetch(job_id)


def test_diagnostic_pages_send_only_limit_offset_and_validate_tenant_order() -> None:
    job_id, org_id = uuid4(), uuid4()
    rows = [
        {
            "id": str(uuid4()),
            "organization_id": str(org_id),
            "requested_by_user_id": str(uuid4()),
            "target": "tcp://a.example:443",
            "status": "completed",
            "result_json": {"z": 2, "a": 1},
            "correlation_id": str(uuid4()),
            "created_at": "2026-07-23T10:00:00Z",
            "updated_at": "2026-07-23T10:01:00Z",
        },
        {
            "id": str(uuid4()),
            "organization_id": str(org_id),
            "requested_by_user_id": str(uuid4()),
            "target": "=FORMULA",
            "status": "completed",
            "result_json": None,
            "correlation_id": str(uuid4()),
            "created_at": "2026-07-23T10:00:01Z",
            "updated_at": "2026-07-23T10:01:01Z",
        },
    ]
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        offset = int(request.url.params["offset"])
        return httpx.Response(
            200,
            json={
                "items": rows[offset : offset + 1],
                "limit": 1,
                "offset": offset,
                "snapshot_id": request.url.params["snapshot_id"],
            },
        )

    fetched = client(httpx.MockTransport(handler)).diagnostics(
        job_id,
        organization_id=org_id,
        snapshot_id=uuid4(),
        page_size=1,
        max_pages=3,
        max_rows=3,
        max_total_bytes=50000,
    )
    assert [row.id for row in fetched] == [UUID(rows[0]["id"]), UUID(rows[1]["id"])]
    assert all(
        set(request.url.params) == {"limit", "offset", "snapshot_id"}
        for request in seen
    )
    assert all(str(job_id) in request.url.path for request in seen)


def test_diagnostic_pagination_has_one_absolute_total_deadline(monkeypatch) -> None:
    job_id, org_id = uuid4(), uuid4()
    ticks = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(control_client_module.time, "monotonic", lambda: next(ticks))
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={
                "items": [],
                "limit": 100,
                "offset": 0,
                "snapshot_id": request.url.params["snapshot_id"],
            },
        )
    )
    with pytest.raises(TransientControlError, match="deadline"):
        client(transport).diagnostics(
            job_id,
            organization_id=org_id,
            snapshot_id=uuid4(),
            page_size=100,
            max_pages=2,
            max_rows=10,
            max_total_bytes=50000,
            total_timeout_seconds=1.0,
        )


def test_diagnostics_reject_cross_tenant_duplicate_and_unstable_rows() -> None:
    job_id, org_id, row_id = uuid4(), uuid4(), uuid4()
    base = {
        "id": str(row_id),
        "organization_id": str(org_id),
        "requested_by_user_id": str(uuid4()),
        "target": "x",
        "status": "completed",
        "result_json": None,
        "correlation_id": str(uuid4()),
        "created_at": "2026-07-23T10:00:00Z",
        "updated_at": "2026-07-23T10:00:00Z",
    }
    cases = [base | {"organization_id": str(uuid4())}, base, base]
    for items in ([cases[0]], cases[1:]):
        transport = httpx.MockTransport(
            lambda request, items=items: httpx.Response(
                200,
                json={
                    "items": items,
                    "limit": 100,
                    "offset": 0,
                    "snapshot_id": request.url.params["snapshot_id"],
                },
            )
        )
        with pytest.raises(TransientControlError):
            client(transport).diagnostics(
                job_id,
                organization_id=org_id,
                snapshot_id=uuid4(),
                page_size=100,
                max_pages=2,
                max_rows=10,
                max_total_bytes=50000,
            )


def test_diagnostics_rejects_page_bound_to_different_snapshot() -> None:
    job_id, org_id, requested_snapshot = uuid4(), uuid4(), uuid4()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={
                "items": [],
                "limit": 100,
                "offset": 0,
                "snapshot_id": str(uuid4()),
            },
        )
    )
    with pytest.raises(TransientControlError):
        client(transport).diagnostics(
            job_id,
            organization_id=org_id,
            snapshot_id=requested_snapshot,
            page_size=100,
            max_pages=2,
            max_rows=10,
            max_total_bytes=50000,
        )


def test_diagnostics_rejects_result_json_above_explicit_depth_bound() -> None:
    job_id, org_id, snapshot_id = uuid4(), uuid4(), uuid4()
    nested: dict[str, object] = {}
    for _ in range(32):
        nested = {"child": nested}
    row = {
        "id": str(uuid4()),
        "organization_id": str(org_id),
        "requested_by_user_id": str(uuid4()),
        "target": "depth.example.test",
        "status": "completed",
        "result_json": nested,
        "correlation_id": str(uuid4()),
        "created_at": "2026-07-23T10:00:00Z",
        "updated_at": "2026-07-23T10:00:00Z",
    }
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={
                "items": [row],
                "limit": 100,
                "offset": 0,
                "snapshot_id": str(snapshot_id),
            },
        )
    )
    with pytest.raises(TransientControlError):
        client(transport).diagnostics(
            job_id,
            organization_id=org_id,
            snapshot_id=snapshot_id,
            page_size=100,
            max_pages=2,
            max_rows=10,
            max_total_bytes=50000,
        )


def test_complete_posts_exact_artifact_then_accepts_only_matching_completion() -> None:
    job_id = uuid4()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"export_job_id": str(job_id), "status": "completed"}
        )

    snapshot_id = uuid4()
    client(httpx.MockTransport(handler)).complete(
        job_id,
        snapshot_id=snapshot_id,
        object_key=f"exports/x/{job_id}.csv",
        sha256="b" * 64,
        size=7,
    )
    assert json.loads(seen[0].content) == {
        "snapshot_id": str(snapshot_id),
        "object_key": f"exports/x/{job_id}.csv",
        "object_sha256": "b" * 64,
        "size_bytes": 7,
    }


@pytest.mark.parametrize(
    "status,error",
    [(401, FatalCredentialError), (403, FatalCredentialError), (409, ClaimConflict)],
)
def test_claim_statuses_are_typed_without_reading_error_body(
    status: int, error: type[Exception]
) -> None:
    class Exploding(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError("error body read")

    with pytest.raises(error):
        client(
            httpx.MockTransport(
                lambda request: httpx.Response(status, stream=Exploding())
            )
        ).claim(uuid4())


def test_all_http_errors_are_sanitized_without_exception_graph() -> None:
    sentinel = "SENSITIVE-TRANSPORT-SENTINEL"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError(sentinel)

    with pytest.raises(TransientControlError) as caught:
        client(httpx.MockTransport(handler)).claim(uuid4())
    error = caught.value
    rendered = "".join(traceback.format_exception(error))
    assert sentinel not in rendered
    assert error.__cause__ is None and error.__context__ is None


def test_success_bodies_are_raw_bounded_and_never_decompressed() -> None:
    class Exploding(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError("body read")

    def response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"Content-Encoding": "gzip"}, stream=Exploding()
        )

    with pytest.raises(TransientControlError):
        client(httpx.MockTransport(response)).claim(uuid4())


def test_whole_work_deadline_is_checked_during_slow_raw_body_trickle(
    monkeypatch,
) -> None:
    job_id = uuid4()
    payload = scope(job_id)
    for key in ("status", "object_key", "object_sha256", "size_bytes"):
        payload.pop(key)
    encoded = json.dumps(payload).encode()
    clock = [0.0]
    monkeypatch.setattr(control_client_module.time, "monotonic", lambda: clock[0])

    class SlowTrickle(httpx.SyncByteStream):
        def __iter__(self):
            yield encoded[:10]
            clock[0] = 2.0
            yield encoded[10:]

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=SlowTrickle(),
        )
    )
    with pytest.raises(TransientControlError, match="deadline"):
        client(transport).claim(job_id, deadline=1.0)
