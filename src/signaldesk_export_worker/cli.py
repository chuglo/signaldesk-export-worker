from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from signal import SIGINT, SIGTERM, getsignal, signal
from threading import Event
from types import FrameType
from typing import Any

import boto3
from botocore.config import Config
import redis

from signaldesk_export_worker.consumer import ExportConsumer
from signaldesk_export_worker.control_client import ControlApiClient
from signaldesk_export_worker.object_store import ArtifactStore
from signaldesk_export_worker.processor import ExportProcessor
from signaldesk_export_worker.settings import Settings


def _close_resources(resources: Sequence[Any]) -> int:
    failures = 0
    for resource in resources:
        if resource is None or not hasattr(resource, "close"):
            continue
        try:
            resource.close()
        except BaseException:
            failures += 1
    return failures


def make_stop_handler(stop_event: Event) -> Callable[[int, FrameType | None], None]:
    def stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    return stop


def create_consumer(settings: Settings, stop_event: Event) -> ExportConsumer:
    resources: list[Any] = []
    try:
        broker = redis.Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=False,
            socket_connect_timeout=settings.redis_connect_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            health_check_interval=30,
        )
        resources.append(broker)
        control = ControlApiClient(
            base_url=str(settings.control_api_base_url),
            credential=settings.export_worker_service_credential.get_secret_value(),
            timeout=settings.api_timeout_seconds,
            max_response_bytes=settings.api_max_response_bytes,
        )
        resources.append(control)
        object_client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key.get_secret_value(),
            aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
            config=Config(
                signature_version="s3v4",
                connect_timeout=settings.object_store_timeout_seconds,
                read_timeout=settings.object_store_timeout_seconds,
                retries={"total_max_attempts": 1, "mode": "standard"},
                proxies={},
                s3={"addressing_style": "path"},
            ),
        )
        resources.append(object_client)
        store = ArtifactStore(
            client=object_client,
            bucket=settings.minio_bucket,
            max_bytes=settings.max_export_bytes,
        )
        processor = ExportProcessor(
            control=control,
            store=store,
            page_size=settings.page_size,
            max_pages=settings.max_pages,
            max_rows=settings.max_rows,
            max_diagnostics_bytes=settings.max_diagnostics_response_bytes,
            max_export_bytes=settings.max_export_bytes,
            diagnostics_total_timeout=settings.diagnostics_total_timeout_seconds,
            object_operation_timeout=settings.object_store_timeout_seconds,
            render_timeout=settings.render_timeout_seconds,
        )
        consumer = ExportConsumer(
            settings=settings,
            redis_client=broker,
            processor=processor,
            stop_event=stop_event,
        )
        consumer.control = control  # type: ignore[attr-defined]
        consumer.object_client = object_client  # type: ignore[attr-defined]
        return consumer
    except BaseException as primary:
        failures = _close_resources(list(reversed(resources)))
        if failures:
            primary.add_note(f"{failures} resource cleanup failure(s) occurred")
        raise


def run_worker(
    settings: Settings, *, once: bool = False, stop_event: Event | None = None
) -> None:
    stop = stop_event or Event()
    consumer = create_consumer(settings, stop)
    primary: BaseException | None = None
    try:
        if once:
            consumer.setup()
            consumer.process_once()
        else:
            consumer.run()
    except BaseException as error:
        primary = error
        raise
    finally:
        resources = [
            getattr(consumer, name, None)
            for name in ("control", "object_client", "redis")
        ]
        failures = _close_resources(resources)
        if failures:
            if primary is not None:
                primary.add_note(f"{failures} resource cleanup failure(s) occurred")
            else:
                raise RuntimeError(
                    f"{failures} resource cleanup failure(s) occurred"
                ) from None


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the SignalDesk export worker")
    parser.add_argument(
        "--once", action="store_true", help="process at most one bounded batch and exit"
    )
    args = parser.parse_args(argv)
    stop = Event()
    handler = make_stop_handler(stop)
    previous = {SIGTERM: getsignal(SIGTERM), SIGINT: getsignal(SIGINT)}
    signal(SIGTERM, handler)
    signal(SIGINT, handler)
    try:
        run_worker(Settings(), once=args.once, stop_event=stop)  # type: ignore[call-arg]
    finally:
        signal(SIGTERM, previous[SIGTERM])
        signal(SIGINT, previous[SIGINT])


if __name__ == "__main__":
    main()
