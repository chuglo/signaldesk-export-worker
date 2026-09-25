from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
from typing import Any

from signaldesk_export_worker.control_client import WorkerScope


class ExistingObjectMismatch(RuntimeError):
    pass


class ObjectStoreTransient(RuntimeError):
    pass


class ObjectStoreOwnershipLost(RuntimeError):
    pass


@dataclass(frozen=True)
class ObjectMetadata:
    size: int
    content_type: str
    metadata: dict[str, str]


@dataclass(frozen=True)
class StoredArtifact:
    key: str
    sha256: str
    size: int


def derive_object_key(scope: WorkerScope) -> str:
    expected = f"exports/{scope.organization_id}/{scope.export_job_id}.{scope.format}"
    if (
        scope.expected_object_key != expected
        or "%" in scope.expected_object_key
        or ".." in scope.expected_object_key
        or "\\" in scope.expected_object_key
    ):
        raise ValueError("unconfined export object key")
    return expected


def _canonical_object_key(scope: WorkerScope) -> str:
    return (
        f"exports/.canonical/{scope.organization_id}/"
        f"{scope.export_job_id}/{scope.snapshot_id}.{scope.format}"
    )


class ArtifactStore:
    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        max_bytes: int,
    ) -> None:
        if bucket != "signaldesk-exports" or max_bytes < 1:
            raise ValueError("invalid confined object-store settings")
        self.client = client
        self.bucket = bucket
        self.max_bytes = max_bytes

    def put_or_verify(
        self,
        scope: WorkerScope,
        data: bytes,
        media_type: str,
        *,
        before_create: Callable[[], bool] | None = None,
        before_race_verify: Callable[[], bool] | None = None,
    ) -> StoredArtifact:
        if len(data) > self.max_bytes:
            raise ValueError("export artifact too large")
        key = derive_object_key(scope)
        expected_media_type = {
            "csv": "text/csv; charset=utf-8",
            "json": "application/json",
        }[scope.format]
        if media_type != expected_media_type:
            raise ValueError("invalid export media type")

        identity = {
            "export-id": str(scope.export_job_id),
            "org-id": str(scope.organization_id),
            "correlation-id": str(scope.correlation_id),
            "snapshot-id": str(scope.snapshot_id),
        }
        candidate_digest = hashlib.sha256(data).hexdigest()
        candidate_metadata = {"sha256": candidate_digest, **identity}
        existing = self._head(key)
        if existing is not None and self._is_exact(
            existing, len(data), media_type, candidate_metadata
        ):
            return StoredArtifact(key, candidate_digest, len(data))
        if existing is not None and self._head(_canonical_object_key(scope)) is None:
            raise ExistingObjectMismatch("existing export object does not match")

        canonical = self._canonicalize(
            scope,
            data,
            media_type,
            identity,
            before_create=before_create,
            before_race_verify=before_race_verify,
        )
        digest = hashlib.sha256(canonical).hexdigest()
        metadata = {"sha256": digest, **identity}

        if existing is not None:
            self._verify(existing, len(canonical), media_type, metadata)
            return StoredArtifact(key, digest, len(canonical))
        self._require_ownership(before_create, "object creation ownership lost")
        created = self._conditional_put(key, canonical, media_type, metadata)
        if not created:
            self._require_ownership(
                before_race_verify, "race verification ownership lost"
            )
            winner = self._head(key)
            if winner is None:
                raise ObjectStoreTransient("object store unavailable")
            self._verify(winner, len(canonical), media_type, metadata)
        return StoredArtifact(key, digest, len(canonical))

    def _canonicalize(
        self,
        scope: WorkerScope,
        candidate: bytes,
        media_type: str,
        identity: dict[str, str],
        *,
        before_create: Callable[[], bool] | None,
        before_race_verify: Callable[[], bool] | None,
    ) -> bytes:
        key = _canonical_object_key(scope)
        digest = hashlib.sha256(candidate).hexdigest()
        metadata = self._canonical_metadata(digest, identity)
        existing = self._head(key)
        if existing is None:
            self._require_ownership(
                before_create, "canonical object creation ownership lost"
            )
            if self._conditional_put(key, candidate, media_type, metadata):
                return candidate
            self._require_ownership(
                before_race_verify, "canonical race verification ownership lost"
            )
            existing = self._head(key)
            if existing is None:
                raise ObjectStoreTransient("object store unavailable")

        canonical_digest = self._verify_canonical(existing, media_type, identity)
        if existing.size == len(candidate) and canonical_digest == digest:
            return candidate
        canonical = self._get_bounded(key)
        if (
            len(canonical) != existing.size
            or hashlib.sha256(canonical).hexdigest() != canonical_digest
        ):
            raise ExistingObjectMismatch("canonical export object does not match")
        return canonical

    def _canonical_metadata(
        self,
        digest: str,
        identity: dict[str, str],
    ) -> dict[str, str]:
        metadata = {
            "sha256": digest,
            **identity,
            "canonical-version": "1",
        }
        return metadata

    def _verify_canonical(
        self,
        existing: ObjectMetadata,
        content_type: str,
        identity: dict[str, str],
    ) -> str:
        metadata = existing.metadata
        expected_keys = {
            "sha256",
            "export-id",
            "org-id",
            "correlation-id",
            "snapshot-id",
            "canonical-version",
        }
        digest = metadata.get("sha256", "")
        if (
            existing.size > self.max_bytes
            or existing.content_type != content_type
            or set(metadata) != expected_keys
            or metadata.get("canonical-version") != "1"
            or any(metadata.get(name) != value for name, value in identity.items())
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ExistingObjectMismatch("canonical export object does not match")
        return digest

    @staticmethod
    def _require_ownership(guard: Callable[[], bool] | None, message: str) -> None:
        if guard is not None and not guard():
            raise ObjectStoreOwnershipLost(message)

    def _conditional_put(
        self,
        key: str,
        data: bytes,
        media_type: str,
        metadata: dict[str, str],
    ) -> bool:
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=media_type,
                IfNoneMatch="*",
                Metadata=metadata,
            )
        except Exception as error:
            response = getattr(error, "response", {})
            code = None
            status = None
            if isinstance(response, dict):
                raw_error = response.get("Error")
                raw_response_metadata = response.get("ResponseMetadata")
                if isinstance(raw_error, dict):
                    code = raw_error.get("Code")
                if isinstance(raw_response_metadata, dict):
                    status = raw_response_metadata.get("HTTPStatusCode")
            if str(code) in {"412", "PreconditionFailed"} or status == 412:
                return False
            raise ObjectStoreTransient("object store unavailable") from None
        return True

    @staticmethod
    def _is_exact(
        existing: ObjectMetadata,
        size: int,
        content_type: str,
        metadata: dict[str, str],
    ) -> bool:
        return (
            existing.size == size
            and existing.content_type == content_type
            and existing.metadata == metadata
        )

    @staticmethod
    def _verify(
        existing: ObjectMetadata,
        size: int,
        content_type: str,
        metadata: dict[str, str],
    ) -> None:
        if not ArtifactStore._is_exact(existing, size, content_type, metadata):
            raise ExistingObjectMismatch("existing export object does not match")

    def _head(self, key: str) -> ObjectMetadata | None:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
            raw_metadata = response.get("Metadata")
            if (
                not isinstance(raw_metadata, dict)
                or type(response.get("ContentLength")) is not int
                or response["ContentLength"] < 0
                or not isinstance(response.get("ContentType"), str)
                or not all(
                    isinstance(name, str) and isinstance(value, str)
                    for name, value in raw_metadata.items()
                )
            ):
                raise ExistingObjectMismatch("invalid existing object metadata")
            return ObjectMetadata(
                size=response["ContentLength"],
                content_type=response["ContentType"],
                metadata=dict(raw_metadata),
            )
        except ExistingObjectMismatch:
            raise
        except Exception as error:
            response = getattr(error, "response", {})
            code = (
                response.get("Error", {}).get("Code")
                if isinstance(response, dict)
                and isinstance(response.get("Error", {}), dict)
                else None
            )
            if str(code) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise ObjectStoreTransient("object store unavailable") from None

    def _get_bounded(self, key: str) -> bytes:
        body: Any = None
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response.get("Body") if isinstance(response, dict) else None
            if body is None or not hasattr(body, "read"):
                raise ObjectStoreTransient("object store unavailable")
            data = body.read(self.max_bytes + 1)
            if not isinstance(data, bytes) or len(data) > self.max_bytes:
                raise ExistingObjectMismatch("canonical export object does not match")
            return data
        except (ExistingObjectMismatch, ObjectStoreTransient):
            raise
        except Exception:
            raise ObjectStoreTransient("object store unavailable") from None
        finally:
            if body is not None and hasattr(body, "close"):
                try:
                    body.close()
                except Exception:
                    pass
