from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
import json
from typing import Any, Literal, Sequence
import unicodedata

from signaldesk_export_worker.control_client import DiagnosticRow

FIELDS = (
    "id",
    "organization_id",
    "requested_by_user_id",
    "target",
    "status",
    "result_json",
    "correlation_id",
    "created_at",
    "updated_at",
)


@dataclass(frozen=True)
class RenderedArtifact:
    data: bytes
    media_type: str


def _normalized(row: DiagnosticRow) -> dict[str, Any]:
    result = (
        None
        if row.result_json is None
        else json.loads(
            json.dumps(
                row.result_json,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    )
    return {
        "id": str(row.id),
        "organization_id": str(row.organization_id),
        "requested_by_user_id": str(row.requested_by_user_id),
        "target": row.target,
        "status": row.status,
        "result_json": result,
        "correlation_id": str(row.correlation_id),
        "created_at": row.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": row.updated_at.isoformat().replace("+00:00", "Z"),
    }


def _protect(value: str) -> str:
    if value.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + value
    index = 0
    while index < len(value):
        character = value[index]
        if not (character.isspace() or unicodedata.category(character) in {"Cc", "Cf"}):
            break
        index += 1
    return "'" + value if index < len(value) and value[index] in "=+-@" else value


def render_export(
    rows: Sequence[DiagnosticRow],
    export_format: Literal["csv", "json"],
    *,
    max_bytes: int,
) -> RenderedArtifact:
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    normalized = [_normalized(row) for row in rows]
    if export_format == "json":
        data = (
            json.dumps(
                normalized, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
            + "\n"
        ).encode("utf-8")
        media = "application/json"
    elif export_format == "csv":
        output = StringIO(newline="")
        writer = csv.writer(output, dialect="excel", lineterminator="\r\n")
        writer.writerow(FIELDS)
        for item in normalized:
            values: list[str] = []
            for field in FIELDS:
                value = item[field]
                if field == "result_json":
                    value = (
                        ""
                        if value is None
                        else json.dumps(
                            value,
                            ensure_ascii=False,
                            allow_nan=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    )
                values.append(_protect(str(value)))
            writer.writerow(values)
        data = output.getvalue().encode("utf-8")
        media = "text/csv; charset=utf-8"
    else:
        raise ValueError("unsupported export format")
    if len(data) > max_bytes:
        raise ValueError("rendered export too large")
    return RenderedArtifact(data=data, media_type=media)
