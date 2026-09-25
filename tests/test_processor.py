from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from signaldesk_contracts import ExportRequestedV1

from signaldesk_export_worker.control_client import (
    ClaimConflict,
    CompletionConflict,
    TransientControlError,
    WorkerScope,
)
from signaldesk_export_worker.object_store import (
    ArtifactStore,
    ExistingObjectMismatch,
    StoredArtifact,
)
from signaldesk_export_worker.processor import ExportProcessor
from signaldesk_export_worker.rendering import RenderedArtifact


def event(org=None):
    return ExportRequestedV1(
        schema_version=1,
        event_id=uuid4(),
        event_type="export.requested.v1",
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        organization_id=org or uuid4(),
        export_job_id=uuid4(),
    )


def scope_for(value, *, status="claimed", snapshot_id=None):
    key = f"exports/{value.organization_id}/{value.export_job_id}.csv"
    return WorkerScope(
        export_job_id=value.export_job_id,
        organization_id=value.organization_id,
        requested_by_user_id=uuid4(),
        correlation_id=value.correlation_id,
        format="csv",
        expected_object_key=key,
        snapshot_id=snapshot_id or uuid4(),
        status=status,
        object_key=(key if status == "completed" else None),
        object_sha256=("a" * 64 if status == "completed" else None),
        size_bytes=(7 if status == "completed" else None),
    )


class Control:
    def __init__(self, value, *, claim_error=None, fetched=None, complete_error=None):
        self.value = value
        self.claim_error = claim_error
        self.fetched = fetched
        self.complete_error = complete_error
        self.calls = []

    def claim(self, job, **_kwargs):
        self.calls.append(("claim", job))
        if self.claim_error:
            raise self.claim_error
        return self.value

    def fetch(self, job, **_kwargs):
        self.calls.append(("fetch", job))
        return self.fetched

    def diagnostics(self, job, **kwargs):
        self.calls.append(("diagnostics", job, kwargs))
        return []

    def complete(self, job, **kwargs):
        kwargs.pop("deadline", None)
        self.calls.append(("complete", job, kwargs))
        if self.complete_error:
            raise self.complete_error


class Store:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def put_or_verify(self, scope, data, media, **_kwargs):
        self.calls.append((scope, data, media))
        if self.error:
            raise self.error
        return StoredArtifact(scope.expected_object_key, "b" * 64, len(data))


def processor(control, store):
    return ExportProcessor(
        control=control,
        store=store,
        page_size=100,
        max_pages=2,
        max_rows=10,
        max_diagnostics_bytes=10000,
        max_export_bytes=10000,
        renderer=lambda rows, fmt, max_bytes: RenderedArtifact(
            b"payload", "text/csv; charset=utf-8"
        ),
    )


def test_processor_uses_only_authoritative_scope_and_completes_after_upload() -> None:
    queued = event()
    authoritative = scope_for(queued)
    control, store = Control(authoritative), Store()
    result = processor(control, store).process(queued)
    assert result.action == "ack"
    diagnostics = control.calls[1]
    assert diagnostics[2]["organization_id"] == authoritative.organization_id
    assert diagnostics[2]["snapshot_id"] == authoritative.snapshot_id
    assert control.calls[-1] == (
        "complete",
        queued.export_job_id,
        {
            "snapshot_id": authoritative.snapshot_id,
            "object_key": authoritative.expected_object_key,
            "sha256": "b" * 64,
            "size": 7,
        },
    )
    assert store.calls and control.calls.index(control.calls[-1]) > control.calls.index(
        diagnostics
    )


def test_queue_scope_forgery_is_terminal_before_rows_or_upload() -> None:
    queued = event()
    real_event = queued.model_copy(update={"organization_id": uuid4()})
    control, store = Control(scope_for(real_event)), Store()
    result = processor(control, store).process(queued)
    assert (result.action, result.failure_code) == ("terminal", "scope_mismatch")
    assert [call[0] for call in control.calls] == ["claim"] and store.calls == []


def test_upload_then_completion_timeout_is_recoverable_without_reupload() -> None:
    queued = event()
    claimed = scope_for(queued)
    first_control, store = (
        Control(claimed, complete_error=TransientControlError("unavailable")),
        Store(),
    )
    assert processor(first_control, store).process(queued).action == "transient"
    assert len(store.calls) == 1
    completed = scope_for(queued, status="completed", snapshot_id=claimed.snapshot_id)
    recovery = Control(
        claimed, claim_error=ClaimConflict("duplicate"), fetched=completed
    )
    assert processor(recovery, store).process(queued).action == "ack"
    assert len(store.calls) == 1
    assert [call[0] for call in recovery.calls] == ["claim", "fetch"]


def test_completion_conflict_acks_only_authoritative_completed_state() -> None:
    queued = event()
    claimed = scope_for(queued)
    completed = scope_for(
        queued, status="completed", snapshot_id=claimed.snapshot_id
    ).model_copy(update={"object_sha256": "b" * 64, "size_bytes": 7})
    done = Control(
        claimed, fetched=completed, complete_error=CompletionConflict("race")
    )
    assert processor(done, Store()).process(queued).action == "ack"
    pending = Control(
        claimed, fetched=claimed, complete_error=CompletionConflict("race")
    )
    assert processor(pending, Store()).process(queued).action == "transient"


def test_completion_conflict_never_acks_different_authoritative_artifact() -> None:
    queued = event()
    claimed = scope_for(queued)
    different = scope_for(queued, status="completed", snapshot_id=claimed.snapshot_id)
    control = Control(
        claimed, fetched=different, complete_error=CompletionConflict("race")
    )
    result = processor(control, Store()).process(queued)
    assert (result.action, result.failure_code) == (
        "terminal",
        "completion_artifact_mismatch",
    )


def test_completion_recovery_rejects_different_snapshot() -> None:
    queued = event()
    claimed = scope_for(queued)
    different_snapshot = scope_for(queued, status="completed").model_copy(
        update={"object_sha256": "b" * 64, "size_bytes": 7}
    )
    control = Control(
        claimed,
        fetched=different_snapshot,
        complete_error=CompletionConflict("lost response"),
    )
    result = processor(control, Store()).process(queued)
    assert (result.action, result.failure_code) == ("terminal", "scope_mismatch")


def test_existing_object_mismatch_fails_closed_terminal() -> None:
    queued = event()
    control = Control(scope_for(queued))
    store = Store(ExistingObjectMismatch("bad"))
    result = processor(control, store).process(queued)
    assert (result.action, result.failure_code) == (
        "terminal",
        "existing_object_mismatch",
    )
    assert not any(call[0] == "complete" for call in control.calls)


def test_takeover_before_conditional_put_causes_no_stale_object_mutation() -> None:
    queued = event()
    control = Control(scope_for(queued))

    class MissingS3:
        def __init__(self):
            self.puts = []

        def head_object(self, **kwargs):
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error

        def put_object(self, **kwargs):
            self.puts.append(kwargs)

    s3 = MissingS3()
    store = ArtifactStore(
        client=s3,
        bucket="signaldesk-exports",
        max_bytes=10000,
    )
    stages = []

    def renew(stage: str) -> bool:
        stages.append(stage)
        return stage != "before_object_create"

    result = processor(control, store).process(queued, renew_ownership=renew)
    assert (result.action, result.failure_code) == ("transient", "ownership_lost")
    assert "before_object_create" in stages
    assert s3.puts == []
    assert not any(call[0] == "complete" for call in control.calls)


def test_takeover_after_store_before_complete_causes_no_stale_completion() -> None:
    queued = event()
    control, store = Control(scope_for(queued)), Store()
    stages = []

    def renew(stage: str) -> bool:
        stages.append(stage)
        return stage != "before_control_complete"

    result = processor(control, store).process(queued, renew_ownership=renew)
    assert (result.action, result.failure_code) == ("transient", "ownership_lost")
    assert "before_control_complete" in stages
    assert store.calls
    assert not any(call[0] == "complete" for call in control.calls)
