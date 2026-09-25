from __future__ import annotations

import hashlib
import csv
import json
from io import StringIO
from datetime import datetime, timezone
from threading import Event, Lock, Thread, get_ident
from uuid import uuid4

import pytest

from signaldesk_export_worker.control_client import DiagnosticRow, WorkerScope
from signaldesk_export_worker.object_store import (
    ArtifactStore,
    ExistingObjectMismatch,
    ObjectMetadata,
    StoredArtifact,
    derive_object_key,
)
from signaldesk_export_worker.rendering import render_export


def row(**overrides: object) -> DiagnosticRow:
    org = overrides.pop("organization_id", uuid4())
    values = {
        "id": uuid4(),
        "organization_id": org,
        "requested_by_user_id": uuid4(),
        "target": "tcp://example.test:443",
        "status": "completed",
        "result_json": {"z": 2, "a": 1},
        "correlation_id": uuid4(),
        "created_at": datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 23, 10, 0, 1, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return DiagnosticRow.model_validate(values)


def test_json_rendering_is_deterministic_utf8_and_canonical() -> None:
    item = row(target="café")
    first = render_export([item], "json", max_bytes=100_000)
    second = render_export([item], "json", max_bytes=100_000)
    assert first.data == second.data
    assert first.media_type == "application/json"
    assert first.data.endswith(b"\n") and "café" in first.data.decode()
    assert list(json.loads(first.data)[0]) == [
        "id",
        "organization_id",
        "requested_by_user_id",
        "target",
        "status",
        "result_json",
        "correlation_id",
        "created_at",
        "updated_at",
    ]
    assert b'{"a":1,"z":2}' in first.data


def test_csv_is_rfc4180_and_protects_formula_prefixes_in_string_cells() -> None:
    org = uuid4()
    cells = ["=1+1", "+cmd", "-2", "@SUM(A1)", "\tcmd", "\rcmd", "\ncmd", "safe"]
    artifact = render_export(
        [row(organization_id=org, target=value) for value in cells],
        "csv",
        max_bytes=100_000,
    )
    text = artifact.data.decode("utf-8")
    assert artifact.media_type == "text/csv; charset=utf-8"
    for value in cells[:-1]:
        assert "'" + value in text
    assert ",safe," in text
    assert "\r\n" in text


@pytest.mark.parametrize("formula", ["=1+1", "+cmd", "-2", "@SUM(A1)"])
@pytest.mark.parametrize(
    "prefix",
    [
        "",
        " ",
        "\u00a0",
        "\t",
        "\r\n",
        "\v\f",
        "\u200b",
        "\u2066",
        "\x00",
        " \u200b\t\u2066\u00a0",
    ],
)
def test_csv_protects_formula_after_any_unicode_ignorable_prefix(
    prefix: str, formula: str
) -> None:
    value = prefix + formula
    artifact = render_export([row(target=value)], "csv", max_bytes=100_000)
    parsed = list(csv.reader(StringIO(artifact.data.decode("utf-8"))))
    target_index = parsed[0].index("target")
    assert parsed[1][target_index] == "'" + value


@pytest.mark.parametrize(
    "safe", ["ordinary", " leading ordinary", "\u00a0ordinary", "\u200bordinary"]
)
def test_csv_does_not_modify_safe_ordinary_strings(safe: str) -> None:
    artifact = render_export([row(target=safe)], "csv", max_bytes=100_000)
    parsed = list(csv.reader(StringIO(artifact.data.decode("utf-8"))))
    assert parsed[1][parsed[0].index("target")] == safe


def test_rendering_enforces_output_bound() -> None:
    with pytest.raises(ValueError, match="too large"):
        render_export([row(target="x" * 1000)], "json", max_bytes=100)


def scope(fmt: str = "csv") -> WorkerScope:
    job, org = uuid4(), uuid4()
    return WorkerScope(
        export_job_id=job,
        organization_id=org,
        requested_by_user_id=uuid4(),
        correlation_id=uuid4(),
        format=fmt,
        expected_object_key=f"exports/{org}/{job}.{fmt}",
        snapshot_id=uuid4(),
        status="claimed",
        object_key=None,
        object_sha256=None,
        size_bytes=None,
    )


def test_key_must_equal_independent_authoritative_derivation() -> None:
    value = scope()
    assert derive_object_key(value) == value.expected_object_key
    for bad in (
        "../x",
        f"exports/{value.organization_id}/%2e%2e.csv",
        value.expected_object_key + "/x",
    ):
        with pytest.raises(ValueError):
            derive_object_key(value.model_copy(update={"expected_object_key": bad}))


class FakeS3:
    def __init__(
        self,
        existing: ObjectMetadata | None = None,
        existing_key: str | None = None,
    ) -> None:
        self.existing = existing
        self.existing_key = existing_key
        self.puts: list[dict[str, object]] = []

    def head_object(self, **kwargs: object) -> dict[str, object]:
        if self.existing is None or kwargs.get("Key") != self.existing_key:
            error = RuntimeError("not found")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error
        return {
            "ContentLength": self.existing.size,
            "ContentType": self.existing.content_type,
            "Metadata": self.existing.metadata,
        }

    def put_object(self, **kwargs: object) -> None:
        self.puts.append(kwargs)


def test_store_uploads_only_confined_bucket_key_with_exact_metadata() -> None:
    value = scope()
    body = b"hello"
    fake = FakeS3()
    store = ArtifactStore(
        client=fake,
        bucket="signaldesk-exports",
        max_bytes=100,
    )
    result = store.put_or_verify(value, body, "text/csv; charset=utf-8")
    expected_hash = hashlib.sha256(body).hexdigest()
    assert result.sha256 == expected_hash and result.size == 5
    assert len(fake.puts) == 2
    assert fake.puts[0]["Key"] == (
        f"exports/.canonical/{value.organization_id}/{value.export_job_id}/"
        f"{value.snapshot_id}.csv"
    )
    assert fake.puts[0]["IfNoneMatch"] == "*"
    assert fake.puts[0]["Metadata"]["canonical-version"] == "1"
    assert fake.puts[1] == {
        "Bucket": "signaldesk-exports",
        "Key": value.expected_object_key,
        "Body": body,
        "ContentType": "text/csv; charset=utf-8",
        "IfNoneMatch": "*",
        "Metadata": {
            "sha256": expected_hash,
            "export-id": str(value.export_job_id),
            "org-id": str(value.organization_id),
            "correlation-id": str(value.correlation_id),
            "snapshot-id": str(value.snapshot_id),
        },
    }


def test_existing_exact_object_is_idempotent_and_mismatch_fails_closed() -> None:
    value, body = scope("json"), b"[]\n"
    digest = hashlib.sha256(body).hexdigest()
    metadata = {
        "sha256": digest,
        "export-id": str(value.export_job_id),
        "org-id": str(value.organization_id),
        "correlation-id": str(value.correlation_id),
        "snapshot-id": str(value.snapshot_id),
    }
    exact = FakeS3(
        ObjectMetadata(
            size=len(body), content_type="application/json", metadata=metadata
        ),
        value.expected_object_key,
    )
    ArtifactStore(
        client=exact,
        bucket="signaldesk-exports",
        max_bytes=100,
    ).put_or_verify(value, body, "application/json")
    assert exact.puts == []
    mismatch = FakeS3(
        ObjectMetadata(
            size=len(body),
            content_type="application/json",
            metadata=metadata | {"org-id": str(uuid4())},
        ),
        value.expected_object_key,
    )
    with pytest.raises(ExistingObjectMismatch):
        ArtifactStore(
            client=mismatch,
            bucket="signaldesk-exports",
            max_bytes=100,
        ).put_or_verify(value, body, "application/json")
    assert mismatch.puts == []


def test_existing_object_with_wrong_content_type_fails_closed() -> None:
    value, body = scope("json"), b"[]\n"
    metadata = {
        "sha256": hashlib.sha256(body).hexdigest(),
        "export-id": str(value.export_job_id),
        "org-id": str(value.organization_id),
        "correlation-id": str(value.correlation_id),
        "snapshot-id": str(value.snapshot_id),
    }
    fake = FakeS3(
        ObjectMetadata(size=len(body), content_type="text/plain", metadata=metadata),
        value.expected_object_key,
    )
    with pytest.raises(ExistingObjectMismatch):
        ArtifactStore(
            client=fake,
            bucket="signaldesk-exports",
            max_bytes=100,
        ).put_or_verify(value, body, "application/json")
    assert fake.puts == []


def test_conditional_put_race_reheads_and_accepts_only_exact_winner() -> None:
    value, body = scope("json"), b"[]\n"
    metadata = {
        "sha256": hashlib.sha256(body).hexdigest(),
        "export-id": str(value.export_job_id),
        "org-id": str(value.organization_id),
        "correlation-id": str(value.correlation_id),
        "snapshot-id": str(value.snapshot_id),
    }

    class TwoWriterRace(FakeS3):
        def put_object(self, **kwargs: object) -> None:
            self.puts.append(kwargs)
            if ".canonical/" in str(kwargs["Key"]):
                return
            self.existing = ObjectMetadata(
                size=len(body), content_type="application/json", metadata=metadata
            )
            self.existing_key = value.expected_object_key
            error = RuntimeError("precondition failed")
            error.response = {  # type: ignore[attr-defined]
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            }
            raise error

    fake = TwoWriterRace()
    stored = ArtifactStore(
        client=fake,
        bucket="signaldesk-exports",
        max_bytes=100,
    ).put_or_verify(value, body, "application/json")
    assert stored == StoredArtifact(
        value.expected_object_key, metadata["sha256"], len(body)
    )
    assert len(fake.puts) == 2 and fake.puts[1]["IfNoneMatch"] == "*"


def test_delayed_stale_conditional_write_cannot_poison_rightful_artifact() -> None:
    value = scope("json")
    stale_body = b'[{"snapshot":"stale-but-valid"}]\n'
    rightful_body = b'[{"snapshot":"newer"}]\n'

    class Body:
        def __init__(self, data):
            self.data = data

        def read(self, amount):
            return self.data[:amount]

        def close(self):
            pass

    class DelayedConditionalS3:
        def __init__(self):
            self.objects = {}
            self.lock = Lock()
            self.stale_started = Event()
            self.rightful_saw_missing = Event()
            self.stale_stored = Event()
            self.stale_thread = None

        def head_object(self, *, Bucket, Key):
            wait_for_stale = False
            with self.lock:
                stored = self.objects.get(Key)
                if (
                    stored is None
                    and self.stale_started.is_set()
                    and get_ident() != self.stale_thread
                    and not self.rightful_saw_missing.is_set()
                ):
                    wait_for_stale = True
            if wait_for_stale:
                self.rightful_saw_missing.set()
                assert self.stale_stored.wait(timeout=2)
                stored = None  # Preserve the already-observed missing HEAD result.
            if stored is None:
                error = RuntimeError("not found")
                error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
                raise error
            return {
                "ContentLength": len(stored["body"]),
                "ContentType": stored["content_type"],
                "Metadata": stored["metadata"],
            }

        def get_object(self, *, Bucket, Key):
            with self.lock:
                return {"Body": Body(self.objects[Key]["body"])}

        def put_object(self, *, Bucket, Key, Body, ContentType, IfNoneMatch, Metadata):
            delayed_stale = Body == stale_body and not self.stale_started.is_set()
            if delayed_stale:
                self.stale_thread = get_ident()
                self.stale_started.set()
                assert self.rightful_saw_missing.wait(timeout=2)
            with self.lock:
                if Key in self.objects:
                    error = RuntimeError("precondition failed")
                    error.response = {  # type: ignore[attr-defined]
                        "Error": {"Code": "PreconditionFailed"},
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    }
                    raise error
                self.objects[Key] = {
                    "body": Body,
                    "content_type": ContentType,
                    "metadata": dict(Metadata),
                }
            if delayed_stale:
                self.stale_stored.set()

    client = DelayedConditionalS3()
    store = ArtifactStore(
        client=client,
        bucket="signaldesk-exports",
        max_bytes=1000,
    )
    stale_results = []
    stale_errors = []

    def delayed_stale_write():
        try:
            stale_results.append(
                store.put_or_verify(value, stale_body, "application/json")
            )
        except Exception as error:
            stale_errors.append(error)

    stale = Thread(target=delayed_stale_write)
    stale.start()
    assert client.stale_started.wait(timeout=2)
    rightful = store.put_or_verify(value, rightful_body, "application/json")
    stale.join(timeout=2)

    expected_digest = hashlib.sha256(stale_body).hexdigest()
    assert not stale.is_alive() and stale_errors == []
    assert stale_results == [
        StoredArtifact(value.expected_object_key, expected_digest, len(stale_body))
    ]
    assert rightful == stale_results[0]
    assert client.objects[value.expected_object_key]["body"] == stale_body


def test_store_independently_rejects_wrong_media_type() -> None:
    value = scope("json")
    with pytest.raises(ValueError, match="media type"):
        ArtifactStore(
            client=FakeS3(),
            bucket="signaldesk-exports",
            max_bytes=100,
        ).put_or_verify(value, b"[]\n", "text/csv; charset=utf-8")
