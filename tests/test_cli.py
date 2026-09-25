from __future__ import annotations

from threading import Event

import pytest

from signaldesk_export_worker import cli
from signaldesk_export_worker.settings import Settings


def test_run_worker_once_sets_up_processes_and_closes(monkeypatch):
    calls = []

    class Resource:
        def close(self):
            calls.append("close")

    class Consumer:
        control = Resource()
        redis = Resource()

        def setup(self):
            calls.append("setup")

        def process_once(self):
            calls.append("process")

    monkeypatch.setattr(cli, "create_consumer", lambda settings, stop: Consumer())
    cli.run_worker(object(), once=True, stop_event=Event())
    assert calls == ["setup", "process", "close", "close"]


def test_run_cleanup_attempts_every_close_and_preserves_processing_failure(
    monkeypatch,
) -> None:
    closed = []

    class Resource:
        def __init__(self, name, *, fail=False):
            self.name, self.fail = name, fail

        def close(self):
            closed.append(self.name)
            if self.fail:
                raise RuntimeError("cleanup sentinel")

    class Consumer:
        control = Resource("control", fail=True)
        object_client = Resource("object", fail=True)
        redis = Resource("redis")

        def setup(self):
            pass

        def process_once(self):
            raise ValueError("primary processing failure")

    monkeypatch.setattr(cli, "create_consumer", lambda settings, stop: Consumer())
    with pytest.raises(ValueError, match="primary processing failure") as caught:
        cli.run_worker(object(), once=True, stop_event=Event())

    assert closed == ["control", "object", "redis"]
    assert "cleanup sentinel" not in str(caught.value)


def test_run_cleanup_reports_sanitized_failure_only_after_every_attempt(
    monkeypatch,
) -> None:
    closed = []

    class Resource:
        def __init__(self, name, *, fail=False):
            self.name, self.fail = name, fail

        def close(self):
            closed.append(self.name)
            if self.fail:
                raise RuntimeError("sensitive cleanup detail")

    class Consumer:
        control = Resource("control", fail=True)
        object_client = Resource("object")
        redis = Resource("redis", fail=True)

        def setup(self):
            pass

        def process_once(self):
            pass

    monkeypatch.setattr(cli, "create_consumer", lambda settings, stop: Consumer())
    with pytest.raises(RuntimeError, match="2 resource cleanup failure") as caught:
        cli.run_worker(object(), once=True, stop_event=Event())

    assert closed == ["control", "object", "redis"]
    assert "sensitive cleanup detail" not in str(caught.value)


def test_stop_handler_sets_event() -> None:
    stop = Event()
    cli.make_stop_handler(stop)(15, None)
    assert stop.is_set()


def test_cli_builds_path_style_minio_client_with_one_total_attempt(monkeypatch) -> None:
    captured = {}
    monkeypatch.setenv("HTTP_PROXY", "http://hostile-proxy.invalid:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://hostile-proxy.invalid:8080")

    class Broker:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return object()

    class Boto:
        @staticmethod
        def client(*args, **kwargs):
            captured.update(kwargs)
            return object()

    monkeypatch.setattr(cli.redis, "Redis", Broker)
    monkeypatch.setattr(cli, "boto3", Boto)
    monkeypatch.setattr(cli, "ControlApiClient", lambda **kwargs: object())
    value = Settings(
        redis_url="redis://redis:6379/0",
        control_api_base_url="https://control:8443",
        export_worker_service_credential="export-worker-service-credential-0001",
        consumer_name="worker-cli",
        minio_access_key="synthetic-access",
        minio_secret_key="synthetic-secret",
    )
    cli.create_consumer(value, Event())
    config = captured["config"]
    assert captured["endpoint_url"] == "http://minio:9000"
    assert config.s3 == {"addressing_style": "path"}
    assert config.retries == {"total_max_attempts": 1, "mode": "standard"}
    assert config.proxies == {}


def test_consumer_construction_failure_closes_resources_already_created(
    monkeypatch,
) -> None:
    closed = []

    class Resource:
        def __init__(self, name):
            self.name = name

        def close(self):
            closed.append(self.name)

    class Broker:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return Resource("redis")

    class FailingBoto:
        @staticmethod
        def client(*args, **kwargs):
            raise RuntimeError("construction failed")

    monkeypatch.setattr(cli.redis, "Redis", Broker)
    monkeypatch.setattr(cli, "boto3", FailingBoto)
    monkeypatch.setattr(cli, "ControlApiClient", lambda **kwargs: Resource("control"))
    value = Settings(
        redis_url="redis://redis:6379/0",
        control_api_base_url="https://control:8443",
        export_worker_service_credential="export-worker-service-credential-0001",
        consumer_name="worker-cleanup",
        minio_access_key="synthetic-access",
        minio_secret_key="synthetic-secret",
    )
    with pytest.raises(RuntimeError, match="construction failed"):
        cli.create_consumer(value, Event())
    assert closed == ["control", "redis"]


def test_construction_cleanup_attempts_every_close_and_preserves_primary(
    monkeypatch,
) -> None:
    closed = []

    class Resource:
        def __init__(self, name, *, fail=False):
            self.name, self.fail = name, fail

        def close(self):
            closed.append(self.name)
            if self.fail:
                raise RuntimeError("cleanup sentinel")

    class Broker:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return Resource("redis")

    class FailingBoto:
        @staticmethod
        def client(*args, **kwargs):
            raise ValueError("primary construction failure")

    monkeypatch.setattr(cli.redis, "Redis", Broker)
    monkeypatch.setattr(cli, "boto3", FailingBoto)
    monkeypatch.setattr(
        cli,
        "ControlApiClient",
        lambda **kwargs: Resource("control", fail=True),
    )
    value = Settings(
        redis_url="redis://redis:6379/0",
        control_api_base_url="https://control:8443",
        export_worker_service_credential="export-worker-service-credential-0001",
        consumer_name="worker-cleanup-failure",
        minio_access_key="synthetic-access",
        minio_secret_key="synthetic-secret",
    )

    with pytest.raises(ValueError, match="primary construction failure") as caught:
        cli.create_consumer(value, Event())

    assert closed == ["control", "redis"]
    assert "cleanup sentinel" not in str(caught.value)
