from __future__ import annotations

import logging
import sys
from collections import deque
from pathlib import Path
from typing import Deque, List

from app.core.config import get_settings

# recent lines for dashboard LOGS panel
_LOG_RING: Deque[str] = deque(maxlen=80)


class RingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        _LOG_RING.append(msg)


def get_recent_logs(n: int = 40) -> List[str]:
    if n <= 0:
        return list(_LOG_RING)
    return list(_LOG_RING)[-n:]


def setup_logging() -> logging.Logger:
    settings = get_settings()
    Path("logs").mkdir(exist_ok=True)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = logging.FileHandler("logs/bot.log", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    rh = RingHandler()
    rh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    rh.setLevel(logging.INFO)
    root.addHandler(rh)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    return logging.getLogger("hedge")


logger = logging.getLogger("hedge")
