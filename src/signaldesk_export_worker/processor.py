from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, Literal, Protocol
from uuid import UUID

from signaldesk_contracts import ExportRequestedV1

from signaldesk_export_worker.control_client import (
    ClaimConflict,
    CompletionConflict,
    DiagnosticRow,
    FatalCredentialError,
    NotFoundError,
    ScopeConflict,
    TransientControlError,
    WorkerScope,
)
from signaldesk_export_worker.object_store import (
    ArtifactStore,
    ExistingObjectMismatch,
    ObjectStoreOwnershipLost,
    ObjectStoreTransient,
    StoredArtifact,
    derive_object_key,
)
from signaldesk_export_worker.rendering import RenderedArtifact, render_export


@dataclass(frozen=True)
class ProcessResult:
    action: Literal["ack", "transient", "terminal"]
    failure_code: str | None = None


class ControlClient(Protocol):
    def claim(self, job_id: UUID, *, deadline: float | None = None) -> WorkerScope: ...

    def fetch(self, job_id: UUID, *, deadline: float | None = None) -> WorkerScope: ...

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
        total_timeout_seconds: float,
        deadline: float | None = None,
    ) -> list[DiagnosticRow]: ...

    def complete(
        self,
        job_id: UUID,
        *,
        snapshot_id: UUID,
        object_key: str,
        sha256: str,
        size: int,
        deadline: float | None = None,
    ) -> None: ...


class ExportProcessor:
    """Orchestrates one export using API authority and a renewed Redis lease."""

    def __init__(
        self,
        *,
        control: ControlClient,
        store: ArtifactStore,
        page_size: int,
        max_pages: int,
        max_rows: int,
        max_diagnostics_bytes: int,
        max_export_bytes: int,
        diagnostics_total_timeout: float = 30.0,
        object_operation_timeout: float = 5.0,
        render_timeout: float = 2.0,
        renderer: Callable[..., RenderedArtifact] = render_export,
    ) -> None:
        self.control = control
        self.store = store
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_rows = max_rows
        self.max_diagnostics_bytes = max_diagnostics_bytes
        self.max_export_bytes = max_export_bytes
        self.diagnostics_total_timeout = diagnostics_total_timeout
        self.object_operation_timeout = object_operation_timeout
        self.render_timeout = render_timeout
        self.renderer = renderer

    def process(
        self,
        event: ExportRequestedV1,
        *,
        renew_ownership: Callable[[str], bool] | None = None,
        deadline: float | None = None,
    ) -> ProcessResult:
        renew = renew_ownership or (lambda _stage: True)
        blocked = self._guard(renew, "before_control_claim", deadline)
        if blocked is not None:
            return blocked
        try:
            scope = self.control.claim(event.export_job_id, deadline=deadline)
        except FatalCredentialError:
            raise
        except ClaimConflict:
            blocked = self._guard(renew, "before_claim_recovery_fetch", deadline)
            if blocked is not None:
                return blocked
            try:
                scope = self.control.fetch(event.export_job_id, deadline=deadline)
            except FatalCredentialError:
                raise
            except NotFoundError:
                return ProcessResult("terminal", "job_not_found")
            except (ScopeConflict, TransientControlError):
                return ProcessResult("transient", "control_unavailable")
        except NotFoundError:
            return ProcessResult("terminal", "job_not_found")
        except TransientControlError:
            return ProcessResult("transient", "control_unavailable")

        if not self._matches(event, scope):
            return ProcessResult("terminal", "scope_mismatch")
        try:
            derive_object_key(scope)
        except ValueError:
            return ProcessResult("terminal", "scope_mismatch")
        if scope.status == "completed":
            return ProcessResult("ack")

        blocked = self._guard(renew, "before_diagnostics", deadline)
        if blocked is not None:
            return blocked
        try:
            rows = self.control.diagnostics(
                event.export_job_id,
                organization_id=scope.organization_id,
                snapshot_id=scope.snapshot_id,
                page_size=self.page_size,
                max_pages=self.max_pages,
                max_rows=self.max_rows,
                max_total_bytes=self.max_diagnostics_bytes,
                total_timeout_seconds=self.diagnostics_total_timeout,
                deadline=deadline,
            )
        except FatalCredentialError:
            raise
        except NotFoundError:
            return ProcessResult("terminal", "job_not_found")
        except ScopeConflict:
            return ProcessResult("transient", "scope_conflict")
        except TransientControlError:
            return ProcessResult("transient", "control_unavailable")

        blocked = self._guard(
            renew,
            "before_render",
            deadline,
            required_budget=self.render_timeout,
        )
        if blocked is not None:
            return blocked
        try:
            artifact = self.renderer(
                rows, scope.format, max_bytes=self.max_export_bytes
            )
        except (ValueError, TypeError):
            return ProcessResult("terminal", "export_too_large_or_invalid")
        if deadline is not None and time.monotonic() >= deadline:
            return ProcessResult("transient", "work_deadline_exceeded")

        blocked = self._guard(
            renew,
            "before_object_head",
            deadline,
            required_budget=2 * self.object_operation_timeout,
        )
        if blocked is not None:
            return blocked
        store_blocked: ProcessResult | None = None

        def store_guard(stage: str) -> bool:
            nonlocal store_blocked
            store_blocked = self._guard(
                renew,
                stage,
                deadline,
                required_budget=2 * self.object_operation_timeout,
            )
            return store_blocked is None

        try:
            stored = self.store.put_or_verify(
                scope,
                artifact.data,
                artifact.media_type,
                before_create=lambda: store_guard("before_object_create"),
                before_race_verify=lambda: store_guard("before_object_race_verify"),
            )
        except ObjectStoreOwnershipLost:
            return store_blocked or ProcessResult("transient", "ownership_lost")
        except ExistingObjectMismatch:
            return ProcessResult("terminal", "existing_object_mismatch")
        except ObjectStoreTransient:
            return ProcessResult("transient", "object_store_unavailable")
        if deadline is not None and time.monotonic() >= deadline:
            return ProcessResult("transient", "work_deadline_exceeded")

        blocked = self._guard(renew, "before_control_complete", deadline)
        if blocked is not None:
            return blocked
        try:
            self.control.complete(
                event.export_job_id,
                snapshot_id=scope.snapshot_id,
                object_key=stored.key,
                sha256=stored.sha256,
                size=stored.size,
                deadline=deadline,
            )
        except FatalCredentialError:
            raise
        except CompletionConflict:
            return self._completion_recovery(
                event, scope.snapshot_id, stored, renew, deadline
            )
        except NotFoundError:
            return ProcessResult("terminal", "job_not_found")
        except TransientControlError:
            return ProcessResult("transient", "control_unavailable")
        return ProcessResult("ack")

    def _completion_recovery(
        self,
        event: ExportRequestedV1,
        snapshot_id: UUID,
        stored: StoredArtifact,
        renew: Callable[[str], bool],
        deadline: float | None,
    ) -> ProcessResult:
        blocked = self._guard(renew, "before_completion_recovery_fetch", deadline)
        if blocked is not None:
            return blocked
        try:
            scope = self.control.fetch(event.export_job_id, deadline=deadline)
        except FatalCredentialError:
            raise
        except NotFoundError:
            return ProcessResult("terminal", "job_not_found")
        except (ScopeConflict, TransientControlError):
            return ProcessResult("transient", "completion_conflict")
        if not self._matches(event, scope):
            return ProcessResult("terminal", "scope_mismatch")
        if scope.snapshot_id != snapshot_id:
            return ProcessResult("terminal", "scope_mismatch")
        if scope.status != "completed":
            return ProcessResult("transient", "completion_conflict")
        if (
            scope.object_key,
            scope.object_sha256,
            scope.size_bytes,
        ) != (stored.key, stored.sha256, stored.size):
            return ProcessResult("terminal", "completion_artifact_mismatch")
        return ProcessResult("ack")

    @staticmethod
    def _guard(
        renew: Callable[[str], bool],
        stage: str,
        deadline: float | None,
        *,
        required_budget: float = 0.0,
    ) -> ProcessResult | None:
        if deadline is not None and deadline - time.monotonic() < required_budget:
            return ProcessResult("transient", "work_deadline_exceeded")
        try:
            owned = renew(stage)
        except Exception:
            return ProcessResult("transient", "ownership_fence_unavailable")
        if owned is not True:
            return ProcessResult("transient", "ownership_lost")
        return None

    @staticmethod
    def _matches(event: ExportRequestedV1, scope: WorkerScope) -> bool:
        return (
            scope.export_job_id == event.export_job_id
            and scope.organization_id == event.organization_id
            and scope.correlation_id == event.correlation_id
        )
