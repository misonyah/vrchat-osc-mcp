"""Single-instance guard.

Running more than one vrchat-osc-mcp process at once means multiple OSC
senders, OSCQuery discovery attempts, and (if enabled) OSC receivers all
competing for the same ports and VRChat process — wasteful and prone to
confusing behavior. This lock lets a new process detect that another
instance is already running and exit instead of piling up.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import IO


class InstanceAlreadyRunningError(RuntimeError):
    """Raised when another vrchat-osc-mcp instance already holds the lock."""


class SingleInstanceLock:
    """Cross-platform exclusive lock backed by a lock file in the temp dir.

    The OS releases the underlying lock automatically if the process dies
    without calling release(), so there is no stale-lock cleanup to do.
    """

    def __init__(self, name: str = "vrchat-osc-mcp") -> None:
        self._path = Path(tempfile.gettempdir()) / f"{name}.lock"
        self._fh: IO[str] | None = None

    def acquire(self) -> None:
        fh = open(self._path, "a+")
        try:
            fh.seek(0)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            fh.close()
            raise InstanceAlreadyRunningError(
                f"Another vrchat-osc-mcp instance is already running (lock file: {self._path})."
            ) from e
        self._fh = fh

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
