from __future__ import annotations

from pydantic import SecretStr, ValidationError
import pytest

from signaldesk_export_worker.settings import Settings


BASE = {
    "redis_url": "redis://redis:6379/0",
    "control_api_base_url": "https://control:8443",
    "export_worker_service_credential": "export-worker-service-credential-0001",
    "consumer_name": "export-worker-1",
    "minio_access_key": "synthetic-access-key",
    "minio_secret_key": "synthetic-secret-key",
}


def settings(**overrides: object) -> Settings:
    return Settings(**(BASE | overrides))


def test_settings_are_least_privilege_and_fixture_confined() -> None:
    value = settings()
    assert value.stream_name == "signaldesk:exports"
    assert value.consumer_group == "export-workers"
    assert value.dlq_stream_name == "signaldesk:exports:dlq"
    assert value.minio_endpoint == "http://minio:9000"
    assert value.minio_bucket == "signaldesk-exports"
    assert value.object_prefix == "exports/"
    assert "bff" not in " ".join(type(value).model_fields)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("control_api_base_url", "https://user:pass@control:8443"),
        ("control_api_base_url", "https://control:8443?tenant=x"),
        ("redis_url", "redis://user:pass@redis:6379/0"),
        ("redis_url", "redis://redis:6379/0?secret=x"),
        ("minio_endpoint", "https://minio:9000"),
        ("minio_endpoint", "http://evil:9000"),
        ("minio_bucket", "other"),
        ("object_prefix", "other/"),
    ],
)
def test_settings_reject_unconfined_endpoints(field: str, bad: object) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: bad})


class LeakyUnsupported:
    def __repr__(self) -> str:
        return "LeakyUnsupported(SENTINEL)"

    def __str__(self) -> str:
        return "SENTINEL"


SECRET_REPRESENTATIONS = [
    "SENTINEL value with spaces",
    b"SENTINEL value with spaces",
    bytearray(b"SENTINEL value with spaces"),
    memoryview(b"SENTINEL value with spaces"),
    LeakyUnsupported(),
]


@pytest.mark.parametrize("raw", SECRET_REPRESENTATIONS)
@pytest.mark.parametrize(
    "field",
    [
        "redis_url",
        "control_api_base_url",
        "export_worker_service_credential",
        "minio_access_key",
        "minio_secret_key",
    ],
)
def test_structured_errors_never_leak_secret_input(field: str, raw: object) -> None:
    if isinstance(raw, memoryview):
        assert b"SENTINEL" in raw.tobytes()
    else:
        assert "SENTINEL" in (repr(raw) + str(raw))
    with pytest.raises(ValidationError) as caught:
        settings(**{field: raw})
    error = caught.value
    rendered = (
        str(error)
        + repr(error)
        + error.json(include_input=True)
        + repr(error.errors(include_input=True))
    )
    assert "SENTINEL" not in rendered


def test_secret_model_dump_forms_are_masked() -> None:
    value = settings(
        export_worker_service_credential="SENTINEL-service-credential-000000000000",
        minio_access_key="SENTINEL-access",
        minio_secret_key="SENTINEL-secret",
    )
    assert all(
        isinstance(getattr(value, field), SecretStr)
        for field in (
            "export_worker_service_credential",
            "minio_access_key",
            "minio_secret_key",
        )
    )
    rendered = repr(value.model_dump()) + value.model_dump_json() + repr(value)
    assert "SENTINEL" not in rendered


@pytest.mark.parametrize(
    "raw",
    [
        "https://user:SENTINEL@control:8443",
        b"https://user:SENTINEL@control:8443",
    ],
)
def test_credential_bearing_control_url_is_masked_in_structured_errors(
    raw: object,
) -> None:
    assert "SENTINEL" in str(raw)
    with pytest.raises(ValidationError) as caught:
        settings(control_api_base_url=raw)
    error = caught.value
    rendered = (
        str(error)
        + repr(error)
        + error.json(include_input=True)
        + repr(error.errors(include_input=True))
    )
    assert "SENTINEL" not in rendered


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("minio_access_key", "aa"),
        ("minio_access_key", "a" * 129),
        ("minio_secret_key", "short"),
        ("minio_secret_key", "s" * 129),
        ("minio_access_key", "access key"),
        ("minio_secret_key", "secret\tkey"),
        ("minio_access_key", "clé"),
        ("minio_secret_key", "clé-secrète"),
    ],
)
def test_minio_keys_have_bounded_ascii_non_whitespace_domains(
    field: str, bad: str
) -> None:
    with pytest.raises(ValidationError):
        settings(**{field: bad})


@pytest.mark.parametrize(
    "overrides",
    [
        {"minio_secret_key": BASE["minio_access_key"]},
        {
            "minio_access_key": BASE["export_worker_service_credential"],
        },
        {
            "minio_secret_key": BASE["export_worker_service_credential"],
        },
    ],
)
def test_service_and_object_store_credentials_are_pairwise_distinct(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        settings(**overrides)


def test_stale_idle_and_whole_work_deadline_cover_all_bounded_phases() -> None:
    values = {
        "api_timeout_seconds": 1.0,
        "diagnostics_total_timeout_seconds": 2.0,
        "object_store_timeout_seconds": 1.0,
        "redis_socket_timeout_seconds": 1.0,
        "render_timeout_seconds": 1.0,
    }
    with pytest.raises(ValidationError):
        settings(**values, stale_idle_ms=5100)
    configured = settings(**values, stale_idle_ms=5101)
    assert configured.max_work_seconds == pytest.approx(34.1)
