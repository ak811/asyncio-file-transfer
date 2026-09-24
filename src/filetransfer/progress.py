"""A minimal terminal progress bar."""

from __future__ import annotations

import sys
import time
from typing import TextIO


def human_bytes(count: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(count) < 1024 or unit == "TiB":
            return f"{count:.0f} {unit}" if unit == "B" else f"{count:.1f} {unit}"
        count /= 1024
    raise AssertionError("unreachable")


class ProgressBar:
    """Renders ``done / total`` with throughput on a single terminal line, at most 10 times per second."""

    def __init__(self, total: int, label: str, stream: TextIO = sys.stderr, initial: int = 0) -> None:
        self.total = total
        self.label = label
        self.stream = stream
        self.done = initial
        self._start = time.perf_counter()
        self._baseline = initial
        self._last_draw = 0.0
        self._enabled = stream.isatty()

    def __call__(self, count: int) -> None:
        self.done += count
        now = time.perf_counter()
        if self._enabled and (now - self._last_draw >= 0.1 or self.done >= self.total):
            self._last_draw = now
            self._draw(now)

    def _draw(self, now: float) -> None:
        fraction = self.done / self.total if self.total else 1.0
        width = 30
        filled = int(width * fraction)
        rate = (self.done - self._baseline) / max(now - self._start, 1e-9)
        self.stream.write(f"\r{self.label} [{'#' * filled}{'.' * (width - filled)}] "
                          f"{fraction:6.1%} {human_bytes(self.done)} / {human_bytes(self.total)} "
                          f"{human_bytes(rate)}/s ")
        self.stream.flush()

    def finish(self) -> None:
        if self._enabled:
            self._draw(time.perf_counter())
            self.stream.write("\n")
            self.stream.flush()
