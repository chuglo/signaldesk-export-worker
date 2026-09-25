from __future__ import annotations

import hashlib
from threading import Event
import time
from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError
from redis.exceptions import ResponseError
from signaldesk_contracts import ExportRequestedV1

from signaldesk_export_worker.processor import ExportProcessor
from signaldesk_export_worker.settings import Settings


class TopologyError(RuntimeError):
    pass


_ACK_SCRIPT = """
local p=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #p==0 then return {0,'ownership_lost'} end
if p[1][2]~=ARGV[3] or tostring(p[1][4])~=ARGV[4] then return {0,'ownership_lost'} end
local n=redis.call('XACK',KEYS[1],ARGV[1],ARGV[2])
if n~=1 then return {-1,'ack_failed'} end
return {1,'acknowledged'}
"""
_DLQ_SCRIPT = """
local p=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #p==0 then return {0,'ownership_lost'} end
if p[1][2]~=ARGV[3] or tostring(p[1][4])~=ARGV[4] then return {0,'ownership_lost'} end
redis.call('XADD',KEYS[2],'*','event',ARGV[5],'event_id',ARGV[6],'failure_code',ARGV[7],'source_stream',ARGV[8],'source_id',ARGV[2],'attempt_count',ARGV[4])
local n=redis.call('XACK',KEYS[1],ARGV[1],ARGV[2])
if n~=1 then return {-1,'ack_failed'} end
return {1,'dead_lettered'}
"""
_RENEW_SCRIPT = """
local p=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #p==0 then return {0,'ownership_lost'} end
if p[1][2]~=ARGV[3] or tostring(p[1][4])~=ARGV[4] then return {0,'ownership_lost'} end
local claimed=redis.call('XCLAIM',KEYS[1],ARGV[1],ARGV[3],0,ARGV[2],'IDLE',0,'JUSTID')
if #claimed~=1 or claimed[1]~=ARGV[2] then return {-1,'renew_failed'} end
local q=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[2],ARGV[2],1)
if #q~=1 or q[1][2]~=ARGV[3] or tostring(q[1][4])~=ARGV[4] then return {-1,'renew_failed'} end
return {1,'renewed'}
"""


class ExportConsumer:
    def __init__(
        self,
        *,
        settings: Settings,
        redis_client: Any,
        processor: ExportProcessor,
        stop_event: Event | None = None,
    ) -> None:
        self.settings = settings
        self.redis = redis_client
        self.processor = processor
        self.stop_event = stop_event or Event()
        # The configured name is an operator-facing label, not an ownership token.
        # A fresh suffix prevents a restarted process from being indistinguishable
        # from an earlier incarnation in Redis' pending-entry list.
        self._consumer_identity = f"{settings.consumer_name}:{uuid4().hex}"
        self._ready = False
        self._autoclaim_cursor: bytes | str = "0-0"

    def setup(self) -> None:
        cluster = self.redis.info("cluster")
        enabled = cluster.get("cluster_enabled", cluster.get(b"cluster_enabled"))
        if enabled not in {0, "0", b"0"}:
            raise TopologyError("export worker requires standalone Redis")
        role = self.redis.role()
        role_name = role[0] if role else None
        if role_name not in {"master", b"master"}:
            raise TopologyError("export worker requires standalone primary Redis")
        try:
            self.redis.xgroup_create(
                self.settings.stream_name,
                self.settings.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
        self._ready = True

    def run(self) -> None:
        if not self._ready:
            self.setup()
        while not self.stop_event.is_set():
            self.process_once()

    def process_once(self) -> int:
        if not self._ready:
            raise RuntimeError("consumer setup is required")
        reclaimed = self.redis.xautoclaim(
            self.settings.stream_name,
            self.settings.consumer_group,
            self._consumer_identity,
            min_idle_time=self.settings.stale_idle_ms,
            start_id=self._autoclaim_cursor,
            count=self.settings.batch_size,
        )
        if reclaimed:
            self._autoclaim_cursor = reclaimed[0]
        entries = reclaimed[1] if reclaimed and len(reclaimed) > 1 else []
        if not entries:
            streams = self.redis.xreadgroup(
                self.settings.consumer_group,
                self._consumer_identity,
                {self.settings.stream_name: ">"},
                count=self.settings.batch_size,
                block=self.settings.block_time_ms,
            )
            entries = streams[0][1] if streams else []
        processed = 0
        for entry_id, fields in entries:
            if self.stop_event.is_set():
                break
            self._process_entry(entry_id, fields)
            processed += 1
        return processed

    def _process_entry(self, entry_id: bytes | str, fields: dict[Any, Any]) -> None:
        deadline = time.monotonic() + self.settings.max_work_seconds
        attempts = self._delivery_count(entry_id)
        raw = {self._name(k): self._bytes(v) for k, v in fields.items()}
        raw_event = raw.get("event", b"")
        parsed: ExportRequestedV1 | None = None
        failure = None
        if set(raw) != {"event", "event_id"}:
            failure = "invalid_stream_fields"
        elif len(raw_event) > self.settings.event_max_bytes:
            failure = "oversized_event"
        elif len(raw["event_id"]) > 64:
            failure = "invalid_event_id"
        else:
            try:
                parsed = ExportRequestedV1.model_validate_json(raw_event)
            except (ValidationError, ValueError):
                failure = "invalid_event"
            if parsed is not None:
                try:
                    field_id = UUID(raw["event_id"].decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    failure = "invalid_event_id"
                else:
                    if field_id != parsed.event_id:
                        failure = "event_id_mismatch"
        if failure:
            self._dead_letter(entry_id, raw_event, parsed, failure, attempts)
            return
        assert parsed is not None
        result = self.processor.process(
            parsed,
            renew_ownership=lambda _stage: self._renew(entry_id, attempts),
            deadline=deadline,
        )
        if result.action == "ack":
            self._ack(entry_id, attempts)
        elif result.action == "terminal":
            self._dead_letter(
                entry_id, raw_event, parsed, result.failure_code or "terminal", attempts
            )
        elif result.action == "transient":
            if attempts >= self.settings.max_deliveries:
                self._dead_letter(
                    entry_id,
                    raw_event,
                    parsed,
                    result.failure_code or "transient",
                    attempts,
                )
        else:
            raise RuntimeError("invalid processor outcome")

    def _delivery_count(self, entry_id: bytes | str) -> int:
        pending = self.redis.xpending_range(
            self.settings.stream_name,
            self.settings.consumer_group,
            min=entry_id,
            max=entry_id,
            count=1,
        )
        if not pending:
            return 1
        value = pending[0].get("times_delivered", pending[0].get(b"times_delivered", 1))
        return max(1, int(value))

    def _ack(self, entry_id: bytes | str, attempts: int) -> bool:
        source = entry_id.decode("ascii") if isinstance(entry_id, bytes) else entry_id
        result = self.redis.eval(
            _ACK_SCRIPT,
            1,
            self.settings.stream_name,
            self.settings.consumer_group,
            source,
            self._consumer_identity,
            str(attempts),
        )
        return self._atomic(result, "acknowledged", "acknowledge")

    def _renew(self, entry_id: bytes | str, attempts: int) -> bool:
        source = entry_id.decode("ascii") if isinstance(entry_id, bytes) else entry_id
        result = self.redis.eval(
            _RENEW_SCRIPT,
            1,
            self.settings.stream_name,
            self.settings.consumer_group,
            source,
            self._consumer_identity,
            str(attempts),
        )
        return self._atomic(result, "renewed", "renew ownership of")

    def _dead_letter(
        self,
        entry_id: bytes | str,
        raw_event: bytes,
        event: ExportRequestedV1 | None,
        code: str,
        attempts: int,
    ) -> bool:
        source = entry_id.decode("ascii") if isinstance(entry_id, bytes) else entry_id
        if event is None:
            safe_event = "sha256:" + hashlib.sha256(raw_event).hexdigest()
            safe_id = "invalid"
        else:
            safe_event = event.model_dump_json()
            safe_id = str(event.event_id)
        result = self.redis.eval(
            _DLQ_SCRIPT,
            2,
            self.settings.stream_name,
            self.settings.dlq_stream_name,
            self.settings.consumer_group,
            source,
            self._consumer_identity,
            str(attempts),
            safe_event,
            safe_id,
            code,
            self.settings.stream_name,
        )
        return self._atomic(result, "dead_lettered", "dead-letter")

    @staticmethod
    def _atomic(result: Any, success: str, operation: str) -> bool:
        message = f"Redis did not atomically {operation} the export event"
        if not isinstance(result, (list, tuple)) or len(result) != 2:
            raise RuntimeError(message)
        status, token = result
        if type(status) is not int or not isinstance(token, (bytes, str)):
            raise RuntimeError(message)
        try:
            text = token.decode("ascii") if isinstance(token, bytes) else token
        except UnicodeDecodeError:
            raise RuntimeError(message) from None
        if status == 0 and text == "ownership_lost":
            return False
        if status == 1 and text == success:
            return True
        raise RuntimeError(message)

    @staticmethod
    def _name(value: Any) -> str:
        if isinstance(value, bytes):
            try:
                return value.decode("ascii")
            except UnicodeDecodeError:
                return "<invalid>"
        return str(value)

    @staticmethod
    def _bytes(value: Any) -> bytes:
        return (
            value if isinstance(value, bytes) else str(value).encode("utf-8", "replace")
        )
