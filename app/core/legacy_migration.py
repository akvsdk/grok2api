"""
Legacy data migrations for local deployments (python/docker).

Goal: when upgrading the project, old on-disk data should still be readable and not lost.
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict

from app.core.logger import logger


def migrate_legacy_cache_dirs(data_dir: Path | None = None) -> Dict[str, Any]:
    """
    Migrate old cache directory layout:

    - legacy: data/temp/{image,video}
    - current: data/tmp/{image,video}

    This keeps existing cached files (not yet cleaned) available after upgrades.
    """

    data_root = data_dir or (Path(__file__).parent.parent.parent / "data")
    legacy_root = data_root / "temp"
    current_root = data_root / "tmp"

    if not legacy_root.exists() or not legacy_root.is_dir():
        return {"migrated": False, "reason": "no_legacy_dir"}

    lock_dir = data_root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)

    done_marker = lock_dir / "legacy_cache_dirs_v1.done"
    if done_marker.exists():
        return {"migrated": False, "reason": "already_done"}

    lock_file = lock_dir / "legacy_cache_dirs_v1.lock"

    # Best-effort cross-process lock (works on Windows/Linux).
    fd: int | None = None
    try:
        try:
            fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            # Another worker/process is migrating. Wait briefly for completion.
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if done_marker.exists():
                    return {"migrated": False, "reason": "waited_for_other_process"}
                time.sleep(0.2)
            return {"migrated": False, "reason": "lock_timeout"}

        current_root.mkdir(parents=True, exist_ok=True)

        moved = 0
        skipped = 0
        errors = 0

        for sub in ("image", "video"):
            src_dir = legacy_root / sub
            if not src_dir.exists() or not src_dir.is_dir():
                continue

            dst_dir = current_root / sub
            dst_dir.mkdir(parents=True, exist_ok=True)

            for item in src_dir.iterdir():
                if not item.is_file():
                    continue
                target = dst_dir / item.name
                if target.exists():
                    skipped += 1
                    continue
                try:
                    shutil.move(str(item), str(target))
                    moved += 1
                except Exception:
                    errors += 1

        # Cleanup empty legacy dirs (best-effort).
        for sub in ("image", "video"):
            p = legacy_root / sub
            try:
                if p.exists() and p.is_dir() and not any(p.iterdir()):
                    p.rmdir()
            except Exception:
                pass
        try:
            if legacy_root.exists() and legacy_root.is_dir() and not any(legacy_root.iterdir()):
                legacy_root.rmdir()
        except Exception:
            pass

        if errors == 0:
            done_marker.write_text(str(int(time.time())), encoding="utf-8")
        if moved or skipped or errors:
            logger.info(
                f"Legacy cache migration complete: moved={moved}, skipped={skipped}, errors={errors}"
            )
        return {"migrated": True, "moved": moved, "skipped": skipped, "errors": errors}
    finally:
        try:
            if fd is not None:
                os.close(fd)
        except Exception:
            pass
        try:
            if lock_file.exists():
                lock_file.unlink()
        except Exception:
            pass


__all__ = ["migrate_legacy_cache_dirs"]
