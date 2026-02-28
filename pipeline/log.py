# pipeline/log.py
#
# Shared pipeline logger with elapsed time.
#
# Design decisions:
#   - Single log() function replaces duplicate _log() helpers across modules.
#   - Elapsed time is shown so the operator can see how long each phase takes.
#   - No external dependencies — plain stdout with flush for immediate visibility.
#   - Thread-safe: sys.stdout.write of a single string is atomic in CPython.
from __future__ import annotations

import sys
import time

_start = time.monotonic()


def log(message: str) -> None:
    """Write a timestamped log line to stdout."""
    elapsed = time.monotonic() - _start
    minutes, seconds = divmod(int(elapsed), 60)
    sys.stdout.write(f"[pipeline {minutes:02d}:{seconds:02d}] {message}\n")
    sys.stdout.flush()
