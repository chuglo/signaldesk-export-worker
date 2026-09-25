from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import time
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationError,
    field_validator,
    model_validator,
)

from signaldesk_export_worker._deadline_http import Deadline, DeadlineTransport


class ControlError(RuntimeError):
    pass


class FatalCredentialError(ControlError):
    pass


class NotFoundError(ControlError):
    pass


class ClaimConflict(ControlError):
    pass


class CompletionConflict(ControlError):
    pass


class ScopeConflict(ControlError):
    pass


class TransientControlError(ControlError):
    pass


_MAX_JSON_DEPTH = 32


def _valid_json_shape(value: JsonValue) -> bool:
    stack: list[tuple[JsonValue, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if depth > _MAX_JSON_DEPTH:
            return False
        if isinstance(item, dict):
            stack.extend((nested, depth + 1) for nested in item.values())
        elif isinstance(item, list):
            stack.extend((nested, depth + 1) for nested in item)
        elif item is not None and not isinstance(item, (str, int, float, bool)):
            return False
    return True


class DiagnosticRow(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    target: str = Field(min_length=1, max_length=2048)
    status: str = Field(min_length=1, max_length=32)
    result_json: dict[str, JsonValue] | None
    correlation_id: UUID
    created_at: datetime
    updated_at: datetime

    @field_validator("result_json")
    @classmethod
    def result_json_is_bounded(
        cls, value: dict[str, JsonValue] | None
    ) -> dict[str, JsonValue] | None:
        if value is not None and not _valid_json_shape(value):
            raise ValueError("diagnostic result JSON exceeds depth bound")
        return value


class WorkerScope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    export_job_id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    correlation_id: UUID
    format: Literal["csv", "json"]
    expected_object_key: str = Field(min_length=1, max_length=128)
    snapshot_id: UUID
    status: Literal["claimed", "completed"]
    object_key: str | None = Field(max_length=128)
    object_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    size_bytes: int | None = Field(default=None, ge=0, le=1_073_741_824)

    @model_validator(mode="after")
    def artifact_matches_status(self) -> "WorkerScope":
        expected = f"exports/{self.organization_id}/{self.export_job_id}.{self.format}"
        if self.expected_object_key != expected:
            raise ValueError("expected object key must be the derived confined path")
        artifact = (self.object_key, self.object_sha256, self.size_bytes)
        if self.status == "claimed" and artifact != (None, None, None):
            raise ValueError("claimed recovery scope must not include artifact")
        if self.status == "completed" and any(value is None for value in artifact):
            raise ValueError("completed recovery scope must include artifact")
        if self.status == "completed" and self.object_key != self.expected_object_key:
            raise ValueError("completed object key must equal expected key")
        return self


class _Claim(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    export_job_id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    correlation_id: UUID
    format: Literal["csv", "json"]
    expected_object_key: str = Field(min_length=1, max_length=128)
    snapshot_id: UUID

    @model_validator(mode="after")
    def expected_key_is_derived(self) -> "_Claim":
        expected = f"exports/{self.organization_id}/{self.export_job_id}.{self.format}"
        if self.expected_object_key != expected:
            raise ValueError("expected object key must be the derived confined path")
        return self


class _Complete(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    export_job_id: UUID
    status: Literal["completed"]


class _Page(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    items: list[DiagnosticRow]
    limit: int
    offset: int
    snapshot_id: UUID


@dataclass(frozen=True)
class _Response:
    status: int
    content: bytes = b""


class ControlApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        credential: str,
        timeout: float,
        max_response_bytes: int = 65_536,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not 256 <= max_response_bytes <= 1_048_576:
            raise ValueError("invalid response byte bound")
        self._cap = max_response_bytes
        self._timeout = timeout
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "Accept-Encoding": "identity",
            "X-SignalDesk-Service-Credential": credential,
        }
        # Injected transports are retained only for deterministic tests. Production
        # requests use a fresh per-request transport carrying one absolute deadline.
        self._client = (
            httpx.Client(
                base_url=self._base_url,
                timeout=httpx.Timeout(timeout),
                transport=transport,
                trust_env=False,
                follow_redirects=False,
                headers=self._headers,
            )
            if transport is not None
            else None
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def claim(self, job_id: UUID, *, deadline: float | None = None) -> WorkerScope:
        response = self._request(
            "POST", f"/internal/exports/{job_id}/claim", deadline=deadline
        )
        self._status(response, ClaimConflict)
        parsed = self._parse(_Claim, response.content)
        if parsed.export_job_id != job_id:
            raise TransientControlError("invalid control API response")
        return WorkerScope(
            **parsed.model_dump(),
            status="claimed",
            object_key=None,
            object_sha256=None,
            size_bytes=None,
        )

    def fetch(self, job_id: UUID, *, deadline: float | None = None) -> WorkerScope:
        response = self._request(
            "GET", f"/internal/exports/{job_id}", deadline=deadline
        )
        self._status(response, ScopeConflict)
        parsed = self._parse(WorkerScope, response.content)
        if parsed.export_job_id != job_id:
            raise TransientControlError("invalid control API response")
        return parsed

    def diagnostics(
        self,
        job_id: UUID,
        *,
        organization_id: UUID,
        snapshot_id: UUID,
        page_size: int,
        max_pages: int,
        max_rows: int,
        max_total_bytes: int,
        total_timeout_seconds: float = 30.0,
        deadline: float | None = None,
    ) -> list[DiagnosticRow]:
        if (
            not 1 <= page_size <= 100
            or max_pages < 1
            or max_rows < 1
            or max_total_bytes < 1
            or total_timeout_seconds <= 0
        ):
            raise ValueError("invalid diagnostics bounds")
        rows: list[DiagnosticRow] = []
        seen: set[UUID] = set()
        previous: tuple[datetime, UUID] | None = None
        offset = 0
        total_bytes = 0
        pagination_deadline = time.monotonic() + total_timeout_seconds
        if deadline is not None:
            pagination_deadline = min(pagination_deadline, deadline)
        for _ in range(max_pages):
            response = self._request(
                "GET",
                f"/internal/exports/{job_id}/diagnostics",
                params={
                    "limit": page_size,
                    "offset": offset,
                    "snapshot_id": str(snapshot_id),
                },
                deadline=pagination_deadline,
            )
            self._status(response, ScopeConflict)
            total_bytes += len(response.content)
            if total_bytes > max_total_bytes:
                raise TransientControlError("diagnostic pages too large")
            page = self._parse(_Page, response.content)
            if (
                page.limit != page_size
                or page.offset != offset
                or page.snapshot_id != snapshot_id
                or len(page.items) > page_size
            ):
                raise TransientControlError("invalid diagnostic pagination")
            for row in page.items:
                order = (row.created_at, row.id)
                if (
                    row.organization_id != organization_id
                    or row.id in seen
                    or (previous is not None and order <= previous)
                ):
                    raise TransientControlError("invalid tenant-scoped diagnostic page")
                seen.add(row.id)
                previous = order
                rows.append(row)
                if len(rows) > max_rows:
                    raise TransientControlError("too many diagnostic rows")
            if len(page.items) < page_size:
                return rows
            offset += len(page.items)
        raise TransientControlError("too many diagnostic pages")

    def complete(
        self,
        job_id: UUID,
        *,
        snapshot_id: UUID,
        object_key: str,
        sha256: str,
        size: int,
        deadline: float | None = None,
    ) -> None:
        response = self._request(
            "POST",
            f"/internal/exports/{job_id}/complete",
            json={
                "snapshot_id": str(snapshot_id),
                "object_key": object_key,
                "object_sha256": sha256,
                "size_bytes": size,
            },
            deadline=deadline,
        )
        self._status(response, CompletionConflict)
        parsed = self._parse(_Complete, response.content)
        if parsed.export_job_id != job_id:
            raise TransientControlError("invalid control API response")

    @staticmethod
    def _parse(model: type[BaseModel], content: bytes) -> Any:
        parsed: Any = None
        try:
            text = content.decode("utf-8", "strict")
            parsed = model.model_validate_json(text)
        except (
            UnicodeDecodeError,
            ValidationError,
            ValueError,
            json.JSONDecodeError,
            RecursionError,
            OverflowError,
        ):
            pass
        if parsed is None:
            raise TransientControlError("invalid control API response")
        return parsed

    def _request(
        self, method: str, path: str, *, deadline: float | None = None, **kwargs: Any
    ) -> _Response:
        expires_at = time.monotonic() + self._timeout
        if deadline is not None:
            expires_at = min(expires_at, deadline)
        operation_deadline = Deadline(expires_at)
        timed_out = False
        try:
            if self._client is not None:
                return self._request_with_client(
                    self._client, operation_deadline, method, path, **kwargs
                )
            with httpx.Client(
                base_url=self._base_url,
                timeout=httpx.Timeout(operation_deadline.remaining()),
                transport=DeadlineTransport(operation_deadline),
                trust_env=False,
                follow_redirects=False,
                headers=self._headers,
            ) as client:
                return self._request_with_client(
                    client, operation_deadline, method, path, **kwargs
                )
        except TransientControlError:
            raise
        except TimeoutError:
            timed_out = True
        except httpx.HTTPError:
            pass
        if timed_out:
            raise TransientControlError("control API work deadline exceeded") from None
        raise TransientControlError("control API unavailable") from None

    def _request_with_client(
        self,
        client: httpx.Client,
        deadline: Deadline,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> _Response:
        deadline.remaining()
        with client.stream(
            method, path, timeout=deadline.remaining(), **kwargs
        ) as response:
            deadline.remaining()
            if response.status_code != 200:
                return _Response(response.status_code)
            encoding = response.headers.get("content-encoding", "").strip().lower()
            if encoding not in {"", "identity"}:
                raise TransientControlError("unsupported control API encoding")
            if (
                response.headers.get("content-type", "").strip().lower()
                != "application/json"
            ):
                raise TransientControlError("invalid control API response")
            declared = response.headers.get("content-length")
            length: int | None = None
            if declared is not None:
                if (
                    len(declared) > 20
                    or not declared.isascii()
                    or not declared.isdigit()
                ):
                    raise TransientControlError("invalid control API response")
                length = int(declared)
                if length > self._cap:
                    raise TransientControlError("control API response too large")
            body = bytearray()
            chunks = (
                (response.content,)
                if response.is_stream_consumed
                else response.iter_raw()
            )
            iterator = iter(chunks)
            while True:
                deadline.remaining()
                chunk: bytes | None = None
                try:
                    chunk = next(iterator)
                except StopIteration:
                    pass
                if chunk is None:
                    deadline.remaining()
                    break
                deadline.remaining()
                if len(body) + len(chunk) > self._cap:
                    raise TransientControlError("control API response too large")
                body.extend(chunk)
            if length is not None and length != len(body):
                raise TransientControlError("invalid control API response")
            return _Response(200, bytes(body))

    @staticmethod
    def _status(response: _Response, conflict: type[ControlError]) -> None:
        if response.status == 200:
            return
        if response.status in {401, 403}:
            raise FatalCredentialError("control API credential denied")
        if response.status == 404:
            raise NotFoundError("export job not found")
        if response.status == 409:
            raise conflict("export state conflict")
        raise TransientControlError("control API request failed")
