from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AnyHttpUrl,
    Field,
    RedisDsn,
    SecretStr,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

ConsumerName = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
]
_REDIS = TypeAdapter(RedisDsn)
_HTTP = TypeAdapter(AnyHttpUrl)
_MASKED_INVALID = "<masked invalid>"
_SECRET_INPUT_FIELDS = (
    "redis_url",
    "control_api_base_url",
    "export_worker_service_credential",
    "minio_access_key",
    "minio_secret_key",
)


class Settings(BaseSettings):
    """Least-privilege settings confined to the synthetic MinIO fixture."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALDESK_EXPORT_WORKER_",
        extra="forbid",
        hide_input_in_errors=True,
        strict=True,
        validate_default=True,
    )

    redis_url: SecretStr
    control_api_base_url: AnyHttpUrl
    export_worker_service_credential: SecretStr
    consumer_name: ConsumerName
    minio_access_key: SecretStr
    minio_secret_key: SecretStr

    stream_name: Literal["signaldesk:exports"] = "signaldesk:exports"
    consumer_group: Literal["export-workers"] = "export-workers"
    dlq_stream_name: Literal["signaldesk:exports:dlq"] = "signaldesk:exports:dlq"
    minio_endpoint: Literal["http://minio:9000"] = "http://minio:9000"
    minio_bucket: Literal["signaldesk-exports"] = "signaldesk-exports"
    object_prefix: Literal["exports/"] = "exports/"

    block_time_ms: int = Field(default=1000, ge=1, le=5000)
    stale_idle_ms: int = Field(default=60_000, ge=1000, le=300_000)
    max_deliveries: int = Field(default=5, ge=1, le=20)
    batch_size: int = Field(default=10, ge=1, le=100)
    event_max_bytes: int = Field(default=8192, ge=256, le=16384)
    redis_connect_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    redis_socket_timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    api_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    api_max_response_bytes: int = Field(default=65_536, ge=256, le=1_048_576)
    diagnostics_total_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    object_store_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    render_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    page_size: int = Field(default=100, ge=1, le=100)
    max_pages: int = Field(default=100, ge=1, le=1000)
    max_rows: int = Field(default=10_000, ge=1, le=100_000)
    max_export_bytes: int = Field(default=16_777_216, ge=1, le=1_073_741_824)
    max_diagnostics_response_bytes: int = Field(
        default=67_108_864, ge=256, le=1_073_741_824
    )

    @model_validator(mode="before")
    @classmethod
    def mask_structured_secrets_before_validation(cls, raw: Any) -> Any:
        if not isinstance(raw, Mapping):
            return raw
        values = dict(raw)
        for field in _SECRET_INPUT_FIELDS:
            value = values.get(field)
            if isinstance(value, SecretStr):
                continue
            if isinstance(value, str):
                text = value
            elif isinstance(value, (bytes, bytearray, memoryview)):
                try:
                    text = bytes(value).decode("utf-8", "strict")
                except UnicodeDecodeError:
                    text = _MASKED_INVALID
            elif value is None:
                continue
            else:
                text = _MASKED_INVALID
            values[field] = SecretStr(text)
        return values

    @field_validator("redis_url")
    @classmethod
    def validate_redis(cls, value: SecretStr) -> SecretStr:
        message = (
            "Redis URL must be an unauthenticated redis URL without query or fragment"
        )
        try:
            parsed = _REDIS.validate_python(value.get_secret_value(), strict=True)
        except (ValidationError, TypeError, ValueError):
            raise ValueError(message) from None
        if (
            parsed.scheme not in {"redis", "rediss"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query is not None
            or parsed.fragment is not None
        ):
            raise ValueError(message) from None
        return value

    @field_validator("control_api_base_url", mode="before")
    @classmethod
    def validate_control_url(cls, value: Any) -> AnyHttpUrl:
        message = "control API URL must be a strict origin without credentials, path, query, or fragment"
        if not isinstance(value, SecretStr):
            value = SecretStr(_MASKED_INVALID)
        try:
            parsed = _HTTP.validate_python(value.get_secret_value(), strict=True)
        except (ValidationError, TypeError, ValueError):
            raise ValueError(message) from None
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.query is not None
            or parsed.fragment is not None
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(message)
        return parsed

    @field_validator("export_worker_service_credential")
    @classmethod
    def validate_credential(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if (
            not 32 <= len(raw) <= 128
            or raw != raw.strip()
            or not raw.isascii()
            or any(ch.isspace() for ch in raw)
        ):
            raise ValueError(
                "export worker credential must be 32 to 128 non-whitespace ASCII characters"
            )
        return value

    @field_validator("minio_access_key")
    @classmethod
    def validate_synthetic_access_key(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if (
            not 3 <= len(raw) <= 128
            or raw != raw.strip()
            or not raw.isascii()
            or any(ch.isspace() for ch in raw)
        ):
            raise ValueError(
                "synthetic MinIO access key must be 3 to 128 non-whitespace ASCII characters"
            )
        return value

    @field_validator("minio_secret_key")
    @classmethod
    def validate_synthetic_secret_key(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if (
            not 8 <= len(raw) <= 128
            or raw != raw.strip()
            or not raw.isascii()
            or any(ch.isspace() for ch in raw)
        ):
            raise ValueError(
                "synthetic MinIO secret key must be 8 to 128 non-whitespace ASCII characters"
            )
        return value

    @model_validator(mode="after")
    def validate_work_window(self) -> Self:
        credentials = {
            self.export_worker_service_credential.get_secret_value(),
            self.minio_access_key.get_secret_value(),
            self.minio_secret_key.get_secret_value(),
        }
        if len(credentials) != 3:
            raise ValueError("service and object-store credentials must be distinct")
        longest_renewed_phase = max(
            4 * self.api_timeout_seconds,
            self.diagnostics_total_timeout_seconds,
            self.render_timeout_seconds,
            2 * self.object_store_timeout_seconds,
        )
        minimum = (
            math.ceil(
                (self.redis_socket_timeout_seconds + longest_renewed_phase) * 1000
            )
            + 100
        )
        if self.stale_idle_ms <= minimum:
            raise ValueError(
                "stale_idle_ms must exceed each renewed external phase, Redis timeout, and 100ms margin"
            )
        return self

    @property
    def max_work_seconds(self) -> float:
        """Conservative whole-message deadline including every worst-case phase."""
        return (
            16 * self.api_timeout_seconds
            + self.diagnostics_total_timeout_seconds
            + self.render_timeout_seconds
            + 6 * self.object_store_timeout_seconds
            + 9 * self.redis_socket_timeout_seconds
            + 0.1
        )
