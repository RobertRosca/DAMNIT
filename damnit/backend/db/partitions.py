"""Lazy creation of per-proposal partitions."""
import logging
import threading

from sqlalchemy import text
from sqlalchemy.orm import Session

from .engine import pg_advisory_xact_lock
from .models import PARTITIONED_TABLES

log = logging.getLogger(__name__)

_ADVISORY_KEY = 0x44414D4E  # "DAMN" as 4 ASCII bytes, arbitrary namespace.

_lock = threading.Lock()
_seen: set[int] = set()


def reset_cache() -> None:
    """Forget which partitions we've already created. Used by tests."""
    with _lock:
        _seen.clear()


def ensure_proposal_partition(session: Session, proposal: int) -> None:
    """Create partitions for the given proposal if they don't exist yet."""
    if proposal is None:
        return
    with _lock:
        if proposal in _seen:
            return
    pg_advisory_xact_lock(session, _ADVISORY_KEY, int(proposal))
    for parent in PARTITIONED_TABLES:
        partition_name = f"{parent}_p{proposal}"
        session.execute(text(
            f'CREATE TABLE IF NOT EXISTS "{partition_name}" '
            f'PARTITION OF "{parent}" FOR VALUES IN ({int(proposal)})'
        ))
    session.flush()
    with _lock:
        _seen.add(proposal)
    log.debug("Ensured partitions for proposal %d", proposal)
