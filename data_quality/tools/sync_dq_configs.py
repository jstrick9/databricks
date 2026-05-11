from __future__ import annotations

import os
import re
import uuid
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import List, Dict, Optional, Tuple

import yaml
from databricks.sdk import WorkspaceClient

from utils.path_utils import get_repo_root


@dataclass(frozen=True)
class SyncResult:
    scanned: int
    copied: int
    skipped_unchanged: int
    skipped_invalid: int
    errors: int
    details: List[Dict]


def _w() -> WorkspaceClient:
    return WorkspaceClient()


def _sha256(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_yaml(path: str) -> bool:
    return path.lower().endswith((".yml", ".yaml"))


def _safe_parse_yaml(text: str) -> Optional[dict]:
    try:
        return yaml.safe_load(text)
    except Exception:
        return None


def _normalize_checks_yaml(doc: dict) -> dict:
    """
    Normalize common variants into a consistent structure that your engine supports:
      - criticality: warn -> warning
      - arguments: {column: X} -> {col_name: X}
      - ensures nested 'check' exists
    """
    if not doc:
        return {"checks": []}

    checks = doc.get("checks", doc) if isinstance(doc, dict) else doc
    if not isinstance(checks, list):
        return doc

    out = []
    for item in checks:
        if not isinstance(item, dict):
            continue

        crit = item.get("criticality", "warning")
        if crit == "warn":
            crit = "warning"
        item["criticality"] = crit

        check_def = item.get("check", item)
        args = check_def.get("arguments") or {}
        if isinstance(args, dict):
            if "column" in args and "col_name" not in args:
                args["col_name"] = args.pop("column")
            if "columns" in args and "col_names" not in args:
                args["col_names"] = args.pop("columns")
            check_def["arguments"] = args

        if "check" not in item:
            item = {"check": check_def, "criticality": crit}

        out.append(item)

    return {"checks": out}


# =========================
# Files API (UC Volumes)
# =========================
def _list_recursive_files_api(root: str) -> List[str]:
    out = []
    stack = [root.rstrip("/")]

    while stack:
        cur = stack.pop()
        try:
            entries = _w().files.list_directory_contents(cur)
        except Exception:
            continue

        for e in entries:
            p = e.path
            if _is_yaml(p):
                out.append(p)
            else:
                stack.append(p)

    return sorted(list(set(out)))


def _download_volume_text(path: str) -> str:
    resp = _w().files.download(path)
    return resp.contents.read().decode("utf-8")


def _upload_volume_text(path: str, text: str, overwrite: bool = True) -> None:
    parent = path.rsplit("/", 1)[0]
    _w().files.create_directory(parent)
    bio = BytesIO(text.encode("utf-8"))
    _w().files.upload(file_path=path, contents=bio, overwrite=overwrite)


# =========================
# Workspace file IO (/Workspace/.../data_quality runtime)
# =========================
def _ensure_local_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_local_text(path: str) -> Optional[str]:
    try:
        with open(path, "r") as f:
            return f.read()
    except Exception:
        return None


def _write_local_text(path: str, text: str) -> None:
    parent = os.path.dirname(path)
    _ensure_local_dir(parent)
    with open(path, "w") as f:
        f.write(text)


# =========================
# Header + archive + audit log
# =========================
def _strip_existing_sync_header(text: str) -> str:
    """
    Remove an existing dq_config_sync header block (comment-only prefix) if present.
    Prevents accumulating headers across sync runs.
    """
    if not text:
        return text

    lines = text.splitlines()
    if not lines or not lines[0].startswith("#"):
        return text

    idx = 0
    header_lines = []
    while idx < len(lines) and lines[idx].startswith("#"):
        header_lines.append(lines[idx])
        idx += 1

    if any("dq_config_sync" in l for l in header_lines):
        while idx < len(lines) and lines[idx].strip() == "":
            idx += 1
        remaining = "\n".join(lines[idx:])
        return remaining + ("\n" if remaining and not remaining.endswith("\n") else "")

    return text


def _build_header_comment(
    source_path: str,
    target_path: str,
    source_sha256: str,
    target_sha256_before: Optional[str],
    normalized: bool,
    synced_by: str,
    synced_at: datetime,
    config_kind: str,
    archive_path: Optional[str],
) -> str:
    payload = {
        "dq_config_sync": {
            "synced_at_utc": synced_at.isoformat(),
            "synced_by": synced_by,
            "config_kind": config_kind,
            "source_path": source_path,
            "source_sha256": source_sha256,
            "target_path": target_path,
            "target_sha256_before": target_sha256_before,
            "normalized": normalized,
            "archive_path": archive_path,
            "ci_commit_sha": os.environ.get("CI_COMMIT_SHA") or os.environ.get("GIT_COMMIT_SHA"),
        }
    }

    header_lines = ["# " + line for line in yaml.dump(payload, sort_keys=False).splitlines()]
    return "\n".join(header_lines) + "\n"


def _archive_old_target_to_volume(
    target_path: str,
    target_text_before: str,
    archive_volume_root: str,
    now: datetime,
) -> str:
    """
    Archive the previous runtime version to a UC Volume using Files API.
    """
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    # Make archive path mirror relative structure under data_quality/
    # Example rel: checks/trm_reporting/silver/table_checks.yml
    if "/data_quality/" in target_path:
        rel = target_path.split("/data_quality/", 1)[1].lstrip("/")
    else:
        rel = os.path.basename(target_path)

    archive_path = f"{archive_volume_root.rstrip('/')}/{ts}/{rel}"
    _upload_volume_text(archive_path, target_text_before, overwrite=True)
    return archive_path


def _log_sync_event(log_table_fqn: str, row: dict) -> None:
    """
    Append audit row to Delta (best effort; never blocks sync).
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.createDataFrame([row]).write.mode("append").saveAsTable(log_table_fqn)
    except Exception as e:
        print(f"WARNING: Failed to write sync log row to {log_table_fqn}: {e}")

_TS_DIR_RE = re.compile(r".*/(\d{8}T\d{6}Z)$")  # .../20260408T153000Z

def _log_purge_event(log_table_fqn: str, row: dict) -> None:
    """
    Append a purge audit row to Delta (best effort; never blocks purge).
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.createDataFrame([row]).write.mode("append").saveAsTable(log_table_fqn)
    except Exception as e:
        print(f"WARNING: Failed to write purge log row to {log_table_fqn}: {e}")

# =========================
# Main sync
# =========================
def sync_volume_to_workspace(
    volume_root: str,
    workspace_dq_root: Optional[str] = None,
    overwrite: bool = True,
    validate_yaml: bool = True,
    normalize_checks: bool = True,
    add_header_comment: bool = True,
    archive_old_versions: bool = True,
    archive_volume_root: Optional[str] = None,
    log_table_fqn: Optional[str] = None,
    dry_run: bool = False,
) -> SyncResult:
    """
    Copy configs from UC Volume -> runtime data_quality folder.

    Source layout:
      <volume_root>/
        checks/<catalog>/<schema>/<table>_checks.yml
        hash_configs/<catalog>/<schema>/<table>_hash_config.yml

    Target layout:
      <workspace_dq_root>/
        checks/<catalog>/<schema>/<table>_checks.yml
        hash_configs/<catalog>/<schema>/<table>_hash_config.yml

    Archive:
      <archive_volume_root>/<timestamp>/<relative_path_under_data_quality>
    """
    if not volume_root.startswith("/Volumes/"):
        raise ValueError(f"volume_root must be /Volumes/... Got: {volume_root}")

    workspace_dq_root = (workspace_dq_root or get_repo_root()).rstrip("/")
    now = datetime.now(timezone.utc)

    # default archive root inside same volume tree
    if archive_volume_root is None:
        archive_volume_root = f"{volume_root.rstrip('/')}/_runtime_archive"

    if not archive_volume_root.startswith("/Volumes/"):
        raise ValueError(f"archive_volume_root must be /Volumes/... Got: {archive_volume_root}")

    # who ran the sync (best effort)
    synced_by = "unknown"
    try:
        synced_by = _w().current_user.me().user_name
    except Exception:
        pass

    src_checks = f"{volume_root.rstrip('/')}/checks"
    src_hash = f"{volume_root.rstrip('/')}/hash_configs"

    tgt_checks = f"{workspace_dq_root}/checks"
    tgt_hash = f"{workspace_dq_root}/hash_configs"

    src_files = _list_recursive_files_api(src_checks) + _list_recursive_files_api(src_hash)

    scanned = copied = skipped_unchanged = skipped_invalid = errors = 0
    details: List[Dict] = []

    for src_path in src_files:
        if not _is_yaml(src_path):
            continue

        scanned += 1

        if "/checks/" in src_path:
            rel = src_path.split("/checks/", 1)[1]
            tgt_path = f"{tgt_checks}/{rel}"
            kind = "checks"
        elif "/hash_configs/" in src_path:
            rel = src_path.split("/hash_configs/", 1)[1]
            tgt_path = f"{tgt_hash}/{rel}"
            kind = "hash_configs"
        else:
            continue

        status = None
        archive_path = None

        try:
            src_text_raw = _download_volume_text(src_path)
        except Exception as e:
            errors += 1
            status = "ERROR_DOWNLOAD"
            details.append({"src": src_path, "tgt": tgt_path, "status": status, "error": str(e)})
            if log_table_fqn:
                _log_sync_event(log_table_fqn, {
                    "sync_id": str(uuid.uuid4()),
                    "synced_at_utc": now,
                    "synced_by": synced_by,
                    "source_path": src_path,
                    "target_path": tgt_path,
                    "config_kind": kind,
                    "status": status,
                    "source_sha256": None,
                    "target_sha256_before": None,
                    "target_sha256_after": None,
                    "bytes_written": 0,
                    "archived_old_version": False,
                    "archive_path": None,
                    "notes": str(e),
                })
            continue

        # Parse/validate
        normalized = False
        src_doc = None
        src_text_for_hashing = src_text_raw

        if validate_yaml:
            src_doc = _safe_parse_yaml(src_text_raw)
            if src_doc is None:
                skipped_invalid += 1
                status = "SKIP_INVALID_YAML"
                details.append({"src": src_path, "tgt": tgt_path, "status": status})
                if log_table_fqn:
                    _log_sync_event(log_table_fqn, {
                        "sync_id": str(uuid.uuid4()),
                        "synced_at_utc": now,
                        "synced_by": synced_by,
                        "source_path": src_path,
                        "target_path": tgt_path,
                        "config_kind": kind,
                        "status": status,
                        "source_sha256": None,
                        "target_sha256_before": None,
                        "target_sha256_after": None,
                        "bytes_written": 0,
                        "archived_old_version": False,
                        "archive_path": None,
                        "notes": "Invalid YAML in source; skipped.",
                    })
                continue

            if normalize_checks and kind == "checks":
                src_doc = _normalize_checks_yaml(src_doc)
                src_text_for_hashing = yaml.dump(src_doc, sort_keys=False)
                normalized = True
            else:
                src_text_for_hashing = yaml.dump(src_doc, sort_keys=False) if isinstance(src_doc, dict) else src_text_raw

        # Compare ignoring header
        tgt_text_before = _read_local_text(tgt_path)
        tgt_sha_before = _sha256(tgt_text_before)
        src_text_stripped = _strip_existing_sync_header(src_text_for_hashing)
        src_sha = _sha256(src_text_stripped)

        if tgt_text_before is not None:
            tgt_stripped = _strip_existing_sync_header(tgt_text_before)
            if tgt_stripped == src_text_stripped:
                skipped_unchanged += 1
                status = "SKIP_UNCHANGED"
                details.append({"src": src_path, "tgt": tgt_path, "status": status})
                if log_table_fqn:
                    _log_sync_event(log_table_fqn, {
                        "sync_id": str(uuid.uuid4()),
                        "synced_at_utc": now,
                        "synced_by": synced_by,
                        "source_path": src_path,
                        "target_path": tgt_path,
                        "config_kind": kind,
                        "status": status,
                        "source_sha256": src_sha,
                        "target_sha256_before": tgt_sha_before,
                        "target_sha256_after": tgt_sha_before,
                        "bytes_written": 0,
                        "archived_old_version": False,
                        "archive_path": None,
                        "notes": "Content identical (ignoring header).",
                    })
                continue

        if dry_run:
            copied += 1
            status = "DRYRUN_COPY"
            details.append({"src": src_path, "tgt": tgt_path, "status": status})
            continue

        # Archive old target version to UC Volume
        archived = False
        if archive_old_versions and tgt_text_before is not None:
            try:
                archive_path = _archive_old_target_to_volume(
                    target_path=tgt_path,
                    target_text_before=tgt_text_before,
                    archive_volume_root=archive_volume_root,
                    now=now,
                )
                archived = True
            except Exception as e:
                # Do not fail sync due to archive issues, but record it
                print(f"WARNING: failed to archive old version for {tgt_path}: {e}")

        # Build final text with header
        final_text = src_text_stripped
        if add_header_comment:
            final_text = _build_header_comment(
                source_path=src_path,
                target_path=tgt_path,
                source_sha256=src_sha or "",
                target_sha256_before=tgt_sha_before,
                normalized=normalized,
                synced_by=synced_by,
                synced_at=now,
                config_kind=kind,
                archive_path=archive_path,
            ) + final_text

        try:
            _write_local_text(tgt_path, final_text)
            tgt_text_after = _read_local_text(tgt_path) or ""
            tgt_sha_after = _sha256(tgt_text_after)
            bytes_written = len(final_text.encode("utf-8"))

            copied += 1
            status = "COPIED"
            details.append({"src": src_path, "tgt": tgt_path, "status": status, "archived": archived, "archive_path": archive_path})

            if log_table_fqn:
                _log_sync_event(log_table_fqn, {
                    "sync_id": str(uuid.uuid4()),
                    "synced_at_utc": now,
                    "synced_by": synced_by,
                    "source_path": src_path,
                    "target_path": tgt_path,
                    "config_kind": kind,
                    "status": status,
                    "source_sha256": src_sha,
                    "target_sha256_before": tgt_sha_before,
                    "target_sha256_after": tgt_sha_after,
                    "bytes_written": bytes_written,
                    "archived_old_version": archived,
                    "archive_path": archive_path,
                    "notes": "",
                })

        except Exception as e:
            errors += 1
            status = "ERROR_WRITE"
            details.append({"src": src_path, "tgt": tgt_path, "status": status, "error": str(e)})
            if log_table_fqn:
                _log_sync_event(log_table_fqn, {
                    "sync_id": str(uuid.uuid4()),
                    "synced_at_utc": now,
                    "synced_by": synced_by,
                    "source_path": src_path,
                    "target_path": tgt_path,
                    "config_kind": kind,
                    "status": status,
                    "source_sha256": src_sha,
                    "target_sha256_before": tgt_sha_before,
                    "target_sha256_after": None,
                    "bytes_written": 0,
                    "archived_old_version": archived,
                    "archive_path": archive_path,
                    "notes": str(e),
                })

    return SyncResult(
        scanned=scanned,
        copied=copied,
        skipped_unchanged=skipped_unchanged,
        skipped_invalid=skipped_invalid,
        errors=errors,
        details=details,
    )


def purge_archives(
    archive_volume_root: str,
    keep_days: int = 90,
    dry_run: bool = True,
    require_suffix: str = "_runtime_archive",
    log_table_fqn: str | None = None,
    log_detail_rows: bool = True
) -> dict:
    """
    Purge old archive folders under a UC Volume archive root.

    Expected structure:
      <archive_volume_root>/
        YYYYMMDDTHHMMSSZ/
          checks/...
          hash_configs/...

    Logging:
      - Writes a SUMMARY row plus optional per-folder detail rows to log_table_fqn.

    Returns:
      dict summary with details list.
    """
    if not archive_volume_root.startswith("/Volumes/"):
        raise ValueError(f"archive_volume_root must start with /Volumes/. Got: {archive_volume_root}")

    if require_suffix and not archive_volume_root.rstrip("/").endswith(require_suffix):
        raise ValueError(
            f"Safety check failed: archive_volume_root must end with '{require_suffix}'. "
            f"Got: {archive_volume_root}"
        )

    w = WorkspaceClient()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=int(keep_days))

    purged_by = "unknown"
    try:
        purged_by = w.current_user.me().user_name
    except Exception:
        pass

    result = {
        "purge_run_id": str(uuid.uuid4()),
        "archive_volume_root": archive_volume_root,
        "keep_days": keep_days,
        "cutoff_utc": cutoff.isoformat(),
        "dry_run": dry_run,
        "scanned": 0,
        "eligible": 0,
        "deleted": 0,
        "skipped": 0,
        "errors": 0,
        "details": []
    }

    # List immediate children under archive root (timestamp folders)
    try:
        entries = w.files.list_directory_contents(archive_volume_root.rstrip("/"))
    except Exception as e:
        raise RuntimeError(f"Could not list archive root: {archive_volume_root}. Error: {e}")

    # Helper to log one row
    def log_row(**kwargs):
        if not log_table_fqn:
            return
        row = {
            "purge_run_id": result["purge_run_id"],
            "event_id": str(uuid.uuid4()),
            "purged_at_utc": now,
            "purged_by": purged_by,
            "archive_volume_root": archive_volume_root,
            "keep_days": int(keep_days),
            "cutoff_utc": cutoff,
            "dry_run": bool(dry_run),

            "folder_path": kwargs.get("folder_path"),
            "folder_timestamp": kwargs.get("folder_timestamp"),
            "action": kwargs.get("action"),
            "reason": kwargs.get("reason"),

            # Summary fields (only for SUMMARY row)
            "scanned": kwargs.get("scanned"),
            "eligible": kwargs.get("eligible"),
            "deleted": kwargs.get("deleted"),
            "skipped": kwargs.get("skipped"),
            "errors": kwargs.get("errors"),
        }
        _log_purge_event(log_table_fqn, row)

    # Process folders
    for e in entries:
        folder_path = e.path
        result["scanned"] += 1

        m = _TS_DIR_RE.match(folder_path.rstrip("/"))
        if not m:
            result["skipped"] += 1
            result["details"].append({"path": folder_path, "action": "SKIP", "reason": "not_timestamp_dir"})
            if log_detail_rows:
                log_row(folder_path=folder_path, action="SKIP", reason="not_timestamp_dir")
            continue

        ts_str = m.group(1)
        try:
            folder_ts = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            result["skipped"] += 1
            result["details"].append({"path": folder_path, "action": "SKIP", "reason": "timestamp_parse_failed"})
            if log_detail_rows:
                log_row(folder_path=folder_path, folder_timestamp=ts_str, action="SKIP", reason="timestamp_parse_failed")
            continue

        # Keep anything newer than cutoff
        if folder_ts >= cutoff:
            result["skipped"] += 1
            result["details"].append({"path": folder_path, "timestamp": ts_str, "action": "KEEP"})
            if log_detail_rows:
                log_row(folder_path=folder_path, folder_timestamp=ts_str, action="KEEP", reason=None)
            continue

        # Eligible for deletion
        result["eligible"] += 1

        if dry_run:
            result["details"].append({"path": folder_path, "timestamp": ts_str, "action": "WOULD_DELETE"})
            if log_detail_rows:
                log_row(folder_path=folder_path, folder_timestamp=ts_str, action="WOULD_DELETE", reason=None)
            continue

        try:
            w.files.delete(folder_path)  # recursive delete
            result["deleted"] += 1
            result["details"].append({"path": folder_path, "timestamp": ts_str, "action": "DELETED"})
            if log_detail_rows:
                log_row(folder_path=folder_path, folder_timestamp=ts_str, action="DELETED", reason=None)
        except Exception as ex:
            result["errors"] += 1
            result["details"].append({"path": folder_path, "timestamp": ts_str, "action": "ERROR", "reason": str(ex)})
            if log_detail_rows:
                log_row(folder_path=folder_path, folder_timestamp=ts_str, action="ERROR", reason=str(ex))

    # Log summary row
    log_row(
        folder_path=None,
        folder_timestamp=None,
        action="SUMMARY",
        reason=None,
        scanned=result["scanned"],
        eligible=result["eligible"],
        deleted=result["deleted"],
        skipped=result["skipped"],
        errors=result["errors"],
    )

    return result