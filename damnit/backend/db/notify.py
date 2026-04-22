"""Postgres LISTEN/NOTIFY plumbing for GUI live updates."""
import json
import logging
import threading
import time
from enum import Enum
from typing import Optional

import psycopg
from sqlalchemy import text
from sqlalchemy.orm import Session

from .engine import database_url


log = logging.getLogger(__name__)

NOTIFY_CHANNEL_FORMAT = "damnit_db_{}"


class MsgKind(Enum):
    """Internal message types for GUI live updates (previously Kafka)."""
    variable_set = "variable_set"
    run_values_updated = "run_values_updated"
    processing_state_set = "processing_state_set"
    processing_finished = "processing_finished"
    # File submission is sent to the combiner via Kafka; kept here for
    # compatibility with existing msg_dict() callers.
    file_submission = "file_submission"


def msg_dict(kind: MsgKind, data: dict) -> dict:
    return {"msg_kind": kind.value, "data": data}


def channel_for(db_id: str) -> str:
    return NOTIFY_CHANNEL_FORMAT.format(db_id)


def notify(session: Session, channel: str, kind: MsgKind, data: dict) -> None:
    payload = json.dumps(msg_dict(kind, data))
    session.execute(
        text("SELECT pg_notify(:chan, :payload)"),
        {"chan": channel, "payload": payload},
    )


def _sync_dsn() -> str:
    """Return a DSN compatible with psycopg.connect (strip SQLAlchemy driver)."""
    url = database_url()
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url[len("postgresql+psycopg://"):]
    if url.startswith("postgresql+"):
        _, _, rest = url.partition("+")
        _, _, rest = rest.partition("://")
        return "postgresql://" + rest
    return url


class PgListener:
    """Blocking listener that yields NOTIFY payloads until :meth:`stop` is called.

    Intended to run inside a dedicated thread; the GUI uses this from a
    Qt worker thread.
    """

    def __init__(self, channel: str, dsn: Optional[str] = None,
                 poll_interval: float = 0.1):
        self.channel = channel
        self._dsn = dsn or _sync_dsn()
        self._poll = poll_interval
        self._stop = threading.Event()
        self._conn: Optional[psycopg.Connection] = None

    def stop(self) -> None:
        self._stop.set()

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self._dsn, autocommit=True)
        # LISTEN needs a valid identifier - channel is built from a hex db_id so
        # quoting is purely defensive.
        conn.execute(f'LISTEN "{self.channel}"')
        return conn

    def iter_notifies(self):
        """Yield (kind, data) pairs until :meth:`stop` is called."""
        self._conn = self._connect()
        try:
            while not self._stop.is_set():
                try:
                    gen = self._conn.notifies(timeout=self._poll)
                    for n in gen:
                        try:
                            msg = json.loads(n.payload)
                        except Exception:
                            log.error("Ignoring malformed NOTIFY payload: %r",
                                      n.payload)
                            continue
                        kind = msg.get("msg_kind")
                        data = msg.get("data", {})
                        yield kind, data
                except psycopg.OperationalError:
                    log.warning("LISTEN connection dropped, reconnecting")
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    time.sleep(1)
                    self._conn = self._connect()
        finally:
            try:
                if self._conn is not None:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None
