from __future__ import annotations

from datetime import datetime, timezone
import time
from uuid import uuid4

import pytest
from signaldesk_contracts import EmailRequestedV1, ExportRequestedV1

from signaldesk_export_worker.consumer import ExportConsumer, TopologyError
from signaldesk_export_worker.control_client import WorkerScope
from signaldesk_export_worker.object_store import ArtifactStore
from signaldesk_export_worker.processor import ExportProcessor
from signaldesk_export_worker.processor import ProcessResult
from signaldesk_export_worker.settings import Settings

CREDENTIAL = "export-worker-service-credential-0001"


def settings(url, consumer, **overrides):
    values = {
        "redis_url": url,
        "control_api_base_url": "https://control:8443",
        "export_worker_service_credential": CREDENTIAL,
        "consumer_name": consumer,
        "minio_access_key": "synthetic-access-key",
        "minio_secret_key": "synthetic-secret-key",
        "api_timeout_seconds": 0.1,
        "diagnostics_total_timeout_seconds": 0.1,
        "object_store_timeout_seconds": 0.1,
        "redis_socket_timeout_seconds": 0.1,
        "render_timeout_seconds": 0.1,
        "stale_idle_ms": 1000,
        "block_time_ms": 10,
        "max_deliveries": 2,
    }
    values.update(overrides)
    return Settings(**values)


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


def add(client, value, **extra):
    fields = {"event": value.model_dump_json(), "event_id": str(value.event_id)}
    fields.update(extra)
    return client.xadd("signaldesk:exports", fields)


class Processor:
    def __init__(self, result=ProcessResult("ack"), fatal=None):
        self.events = []
        self.result = result
        self.fatal = fatal

    def process(self, value, **_kwargs):
        self.events.append(value)
        if self.fatal:
            raise self.fatal
        return self.result


def test_group_new_event_and_ack_happen_only_after_processor(redis_client, redis_url):
    value = event()
    add(redis_client, value)
    processor = Processor()
    consumer = ExportConsumer(
        settings=settings(redis_url, "worker-a"),
        redis_client=redis_client,
        processor=processor,
    )
    consumer.setup()
    assert consumer.process_once() == 1
    assert processor.events == [value]
    assert redis_client.xpending("signaldesk:exports", "export-workers")["pending"] == 0


def test_malformed_unsupported_and_extra_fields_are_safely_dead_lettered(
    redis_client, redis_url
):
    redis_client.xadd(
        "signaldesk:exports",
        {"event": '{"credential":"SENTINEL"}', "event_id": str(uuid4())},
    )
    email = EmailRequestedV1(
        schema_version=1,
        event_id=uuid4(),
        event_type="email.requested.v1",
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        organization_id=uuid4(),
        email_delivery_id=uuid4(),
    )
    redis_client.xadd(
        "signaldesk:exports",
        {"event": email.model_dump_json(), "event_id": str(email.event_id)},
    )
    add(redis_client, event(), organization_id="forged")
    processor = Processor()
    consumer = ExportConsumer(
        settings=settings(redis_url, "worker-poison"),
        redis_client=redis_client,
        processor=processor,
    )
    consumer.setup()
    consumer.process_once()
    records = redis_client.xrange("signaldesk:exports:dlq")
    assert len(records) == 3 and processor.events == []
    assert b"SENTINEL" not in b"".join(
        v for _, fields in records for v in fields.values()
    )


def test_transient_stays_pending_until_delivery_ceiling_then_dlqs(
    redis_client, redis_url
):
    value = event()
    add(redis_client, value)
    processor = Processor(ProcessResult("transient", "api_unavailable"))
    first = ExportConsumer(
        settings=settings(redis_url, "worker-retry-a"),
        redis_client=redis_client,
        processor=processor,
    )
    first.setup()
    first.process_once()
    assert redis_client.xpending("signaldesk:exports", "export-workers")["pending"] == 1
    second = ExportConsumer(
        settings=settings(redis_url, "worker-retry-b"),
        redis_client=redis_client,
        processor=processor,
    )
    claimed = redis_client.xautoclaim(
        "signaldesk:exports",
        "export-workers",
        second._consumer_identity,
        min_idle_time=0,
        start_id="0-0",
        count=1,
    )
    second._ready = True
    second._process_entry(*claimed[1][0])
    assert redis_client.xpending("signaldesk:exports", "export-workers")["pending"] == 0
    assert redis_client.xlen("signaldesk:exports:dlq") == 1


def test_stale_owner_generation_cannot_ack_or_dlq(redis_client, redis_url):
    value = event()
    add(redis_client, value)
    stale = ExportConsumer(
        settings=settings(redis_url, "owner-a"),
        redis_client=redis_client,
        processor=Processor(),
    )
    stale.setup()
    entry = redis_client.xreadgroup(
        "export-workers", "owner-a", {"signaldesk:exports": ">"}, count=1
    )[0][1][0][0]
    assert stale._delivery_count(entry) == 1
    redis_client.xautoclaim(
        "signaldesk:exports",
        "export-workers",
        "owner-b",
        min_idle_time=0,
        start_id="0-0",
        count=1,
    )
    assert stale._ack(entry, 1) is False
    assert (
        stale._dead_letter(entry, value.model_dump_json().encode(), value, "stale", 1)
        is False
    )
    pending = redis_client.xpending_range(
        "signaldesk:exports", "export-workers", min=entry, max=entry, count=1
    )[0]
    assert pending["consumer"] == b"owner-b" and pending["times_delivered"] == 2
    assert redis_client.xlen("signaldesk:exports:dlq") == 0


def test_same_configured_name_cannot_renew_or_ack_a_new_incarnation_generation(
    redis_client, redis_url
):
    value = event()
    add(redis_client, value)

    class RenewingProcessor(Processor):
        renewed = None

        def process(self, parsed, *, renew_ownership, **_kwargs):
            self.renewed = renew_ownership("probe")
            return ProcessResult("ack")

    processor = RenewingProcessor()
    configured = settings(redis_url, "shared-configured-name")
    stale = ExportConsumer(
        settings=configured, redis_client=redis_client, processor=processor
    )
    rightful = ExportConsumer(
        settings=configured, redis_client=redis_client, processor=Processor()
    )
    stale.setup()

    class ReclaimBeforePendingRead:
        def __init__(self, client):
            self.client = client
            self.reclaimed = False

        def __getattr__(self, name):
            return getattr(self.client, name)

        def xpending_range(self, *args, **kwargs):
            if not self.reclaimed:
                self.reclaimed = True
                self.client.xautoclaim(
                    "signaldesk:exports",
                    "export-workers",
                    getattr(
                        rightful,
                        "_consumer_identity",
                        rightful.settings.consumer_name,
                    ),
                    min_idle_time=0,
                    start_id="0-0",
                    count=1,
                )
            return self.client.xpending_range(*args, **kwargs)

    stale.redis = ReclaimBeforePendingRead(redis_client)
    stale.process_once()

    pending = redis_client.xpending_range(
        "signaldesk:exports", "export-workers", min="-", max="+", count=1
    )
    assert processor.renewed is False
    assert len(pending) == 1
    assert pending[0]["consumer"] == getattr(rightful, "_consumer_identity").encode()
    assert pending[0]["times_delivered"] == 2


def test_same_owner_renewal_resets_idle_without_incrementing_generation(
    redis_client, redis_url
):
    value = event()
    add(redis_client, value)
    consumer = ExportConsumer(
        settings=settings(redis_url, "renew-owner"),
        redis_client=redis_client,
        processor=Processor(),
    )
    consumer.setup()
    entry = redis_client.xreadgroup(
        "export-workers",
        consumer._consumer_identity,
        {"signaldesk:exports": ">"},
        count=1,
    )[0][1][0][0]
    time.sleep(0.02)
    assert consumer._renew(entry, 1) is True
    pending = redis_client.xpending_range(
        "signaldesk:exports", "export-workers", min=entry, max=entry, count=1
    )[0]
    assert pending["consumer"] == consumer._consumer_identity.encode()
    assert pending["times_delivered"] == 1


def test_consumer_carries_atomic_renewal_and_whole_deadline_to_processor(
    redis_client, redis_url
):
    value = event()
    add(redis_client, value)

    class InspectingProcessor:
        renewed = False
        remaining = 0.0

        def process(self, parsed, *, renew_ownership, deadline):
            self.remaining = deadline - time.monotonic()
            self.renewed = renew_ownership("before_control_claim")
            return ProcessResult("transient", "probe")

    processor = InspectingProcessor()
    configured = settings(redis_url, "deadline-owner")
    consumer = ExportConsumer(
        settings=configured, redis_client=redis_client, processor=processor
    )
    consumer.setup()
    consumer.process_once()
    assert processor.renewed is True
    assert 0 < processor.remaining <= configured.max_work_seconds
    pending = redis_client.xpending("signaldesk:exports", "export-workers")
    assert pending["pending"] == 1


@pytest.mark.parametrize(("reclaim_at_renewal", "expected_puts"), [(5, 0), (6, 1)])
def test_real_redis_takeover_before_put_or_complete_blocks_stale_mutation(
    redis_client, redis_url, reclaim_at_renewal, expected_puts
):
    value = event()
    add(redis_client, value)
    object_key = f"exports/{value.organization_id}/{value.export_job_id}.csv"
    scope = WorkerScope(
        export_job_id=value.export_job_id,
        organization_id=value.organization_id,
        requested_by_user_id=uuid4(),
        correlation_id=value.correlation_id,
        format="csv",
        expected_object_key=object_key,
        snapshot_id=uuid4(),
        status="claimed",
        object_key=None,
        object_sha256=None,
        size_bytes=None,
    )

    class Control:
        def __init__(self):
            self.completes = []

        def claim(self, job, **kwargs):
            return scope

        def diagnostics(self, job, **kwargs):
            return []

        def complete(self, job, **kwargs):
            self.completes.append(kwargs)

    class MissingS3:
        def __init__(self):
            self.puts = []

        def head_object(self, **kwargs):
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error

        def put_object(self, **kwargs):
            self.puts.append(kwargs)

    class RedisRace:
        def __init__(self, client):
            self.client = client
            self.renewals = 0

        def __getattr__(self, name):
            return getattr(self.client, name)

        def eval(self, script, numkeys, *args):
            if "return {1,'renewed'}" in script:
                self.renewals += 1
                if self.renewals == reclaim_at_renewal:
                    self.client.xautoclaim(
                        "signaldesk:exports",
                        "export-workers",
                        "new-owner",
                        min_idle_time=0,
                        start_id="0-0",
                        count=1,
                    )
            return self.client.eval(script, numkeys, *args)

    control, s3 = Control(), MissingS3()
    processor = ExportProcessor(
        control=control,
        store=ArtifactStore(
            client=s3,
            bucket="signaldesk-exports",
            max_bytes=10_000,
        ),
        page_size=100,
        max_pages=2,
        max_rows=10,
        max_diagnostics_bytes=10_000,
        max_export_bytes=10_000,
        diagnostics_total_timeout=0.1,
        object_operation_timeout=0.1,
        render_timeout=0.1,
    )
    consumer = ExportConsumer(
        settings=settings(redis_url, "stale-owner"),
        redis_client=RedisRace(redis_client),
        processor=processor,
    )
    consumer.setup()
    consumer.process_once()
    assert len(s3.puts) == expected_puts
    assert control.completes == []
    pending = redis_client.xpending_range(
        "signaldesk:exports", "export-workers", min="-", max="+", count=1
    )[0]
    assert pending["consumer"] == b"new-owner"


def test_setup_rejects_cluster_before_mutation() -> None:
    class Cluster:
        def info(self, section):
            return {"cluster_enabled": 1}

    consumer = ExportConsumer(
        settings=settings("redis://redis:6379/0", "cluster-worker"),
        redis_client=Cluster(),
        processor=Processor(),
    )
    with pytest.raises(TopologyError):
        consumer.setup()


def test_xautoclaim_cursor_is_preserved() -> None:
    class Broker:
        def __init__(self):
            self.starts = []
            self.next = iter([b"9-0", b"0-0"])

        def xautoclaim(self, *args, start_id, **kwargs):
            self.starts.append(start_id)
            return [next(self.next), [], []]

        def xreadgroup(self, *args, **kwargs):
            return []

    broker = Broker()
    consumer = ExportConsumer(
        settings=settings("redis://redis:6379/0", "cursor-worker"),
        redis_client=broker,
        processor=Processor(),
    )
    consumer._ready = True
    consumer.process_once()
    consumer.process_once()
    assert broker.starts == ["0-0", b"9-0"]
