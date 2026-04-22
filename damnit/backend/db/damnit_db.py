"""High-level DamnitDB façade over Postgres/SQLAlchemy."""
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex
from typing import Any, Optional

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ...definitions import DEFAULT_CONTEXT_PYTHON
from ..user_variables import UserEditableVariable
from .engine import get_sessionmaker, get_engine
from .models import (DbInfo, ProposalInfo, RunInfo, RunVariable, TimeComment,
                     Variable)
from .notify import MsgKind, channel_for, msg_dict, notify
from .partitions import ensure_proposal_partition
from .values import (BlobTypes, ReducedData, ValueKind, blob2complex,
                     blob2numpy, complex2blob, decode_value, encode_value,
                     numpy2blob)

log = logging.getLogger(__name__)


class _SessionSqlShim:
    """Very small ``conn.execute``/``fetchone`` shim used by legacy tests.

    Rewrites ``?`` placeholders to SQLAlchemy ``:p0``-style, passes positional
    parameters as a dict, and returns rows that support both attribute and
    index access.
    """

    def __init__(self, session: Session):
        self._s = session

    @staticmethod
    def _rewrite(sql: str, params):
        params = params or ()
        if not isinstance(params, (list, tuple)):
            return sql, params
        out = []
        i = 0
        for ch in sql:
            if ch == "?" and i < len(params):
                out.append(f":p{i}")
                i += 1
            else:
                out.append(ch)
        return "".join(out), {f"p{ix}": v for ix, v in enumerate(params)}

    def execute(self, sql, params=()):
        stmt, payload = self._rewrite(sql, params)
        return _CursorShim(self._s.execute(text(stmt), payload))

    def executemany(self, sql, seq):
        for row in seq:
            self.execute(sql, row)
        return _CursorShim(None)


class _CursorShim:
    def __init__(self, result):
        self._r = result

    def fetchone(self):
        if self._r is None:
            return None
        row = self._r.fetchone()
        return tuple(row) if row is not None else None

    def fetchall(self):
        if self._r is None:
            return []
        return [tuple(r) for r in self._r.fetchall()]

    def __iter__(self):
        if self._r is None:
            return iter(())
        return (tuple(r) for r in self._r)


class DamnitDB:
    """Postgres-backed replacement for the old sqlite ``DamnitDB``.

    Parameters
    ----------
    proposal:
        Active proposal. Required for proposal-specific writes.
    session:
        Optional existing SQLAlchemy ``Session`` to reuse. Useful for tests and
        for sharing a connection with callers that already hold one.
    """

    def __init__(self, proposal: Optional[int] = None,
                 *, session: Optional[Session] = None):
        self._proposal: Optional[int] = int(proposal) if proposal is not None else None
        self._external_session = session is not None
        self._sessionmaker = None if session is not None else get_sessionmaker()
        self._session: Session = session or self._sessionmaker()

        self._ensure_db_info()
        if self._proposal is not None:
            self._ensure_proposal(self._proposal)

    # ---------- lifecycle helpers ----------
    def close(self) -> None:
        if not self._external_session:
            self._session.close()

    def __enter__(self) -> "DamnitDB":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    @property
    def session(self) -> Session:
        return self._session

    @property
    def proposal(self) -> Optional[int]:
        return self._proposal

    @property
    def conn(self) -> "_SessionSqlShim":
        """Compatibility shim for callers that still use ``db.conn.execute``.

        Only handles positional-argument queries with a small tuple-like
        result shape. New code should use :attr:`session` or the typed helpers.
        """
        return _SessionSqlShim(self._session)

    # ---------- configuration / metadata ----------
    def _ensure_db_info(self) -> None:
        row = self._session.get(DbInfo, 1)
        if row is None:
            row = DbInfo(id=1, db_id=token_hex(20))
            self._session.add(row)
            self._session.flush()
        self._db_id = row.db_id

    def _ensure_proposal(self, proposal: int) -> None:
        ensure_proposal_partition(self._session, proposal)
        row = self._session.get(ProposalInfo, proposal)
        if row is None:
            self._session.add(ProposalInfo(
                proposal=proposal,
                context_python=DEFAULT_CONTEXT_PYTHON,
            ))
            self._session.flush()

    @property
    def db_id(self) -> str:
        return self._db_id

    @property
    def notify_channel(self) -> str:
        return channel_for(self._db_id)

    # Transitional alias - old code called ``db.kafka_topic``.
    @property
    def kafka_topic(self) -> str:
        return self.notify_channel

    def new_db_id(self) -> str:
        """Assign a fresh install-wide ``db_id``."""
        row = self._session.get(DbInfo, 1)
        row.db_id = token_hex(20)
        self._session.commit()
        self._db_id = row.db_id
        return self._db_id

    def proposal_settings(self) -> dict:
        """Return the ProposalInfo row as a dict (empty dict if missing)."""
        if self._proposal is None:
            return {}
        row = self._session.get(ProposalInfo, self._proposal)
        if row is None:
            return {}
        return {
            "proposal": row.proposal,
            "damnit_python": row.damnit_python,
            "context_python": row.context_python,
            "concurrent_jobs": row.concurrent_jobs,
            "slurm_time": row.slurm_time,
            "slurm_partition": row.slurm_partition,
            "slurm_reservation": row.slurm_reservation,
            "noncluster_cpus": row.noncluster_cpus,
            "noncluster_mem": row.noncluster_mem,
        }

    def get_setting(self, key: str, default=None):
        row = self._session.get(ProposalInfo, self._proposal) if self._proposal is not None else None
        if row is None:
            return default
        value = getattr(row, key, None)
        return default if value is None else value

    def set_setting(self, key: str, value) -> None:
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")
        row = self._session.get(ProposalInfo, self._proposal)
        if row is None:
            row = ProposalInfo(proposal=self._proposal)
            self._session.add(row)
        setattr(row, key, value)
        self._session.commit()

    # ---------- standalone comments ----------
    def add_standalone_comment(self, ts: float, comment: str) -> int:
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")
        obj = TimeComment(proposal=self._proposal, timestamp=ts, comment=comment)
        self._session.add(obj)
        self._session.commit()
        return obj.id

    def change_standalone_comment(self, comment_id: int, comment: str) -> None:
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")
        stmt = (
            TimeComment.__table__
            .update()
            .where(TimeComment.proposal == self._proposal)
            .where(TimeComment.id == comment_id)
            .values(comment=comment)
        )
        self._session.execute(stmt)
        self._session.commit()

    def list_standalone_comments(self):
        """Return [(id, timestamp, comment)] sorted by timestamp desc."""
        if self._proposal is None:
            return []
        stmt = (
            select(TimeComment.id, TimeComment.timestamp, TimeComment.comment)
            .where(TimeComment.proposal == self._proposal)
            .order_by(TimeComment.timestamp.desc())
        )
        return [tuple(r) for r in self._session.execute(stmt).all()]

    # ---------- runs ----------
    def ensure_run(self, proposal: int, run: int,
                   added_at: Optional[float] = None,
                   start_time: Optional[float] = None) -> None:
        if added_at is None:
            added_at = datetime.now(tz=timezone.utc).timestamp()
        ensure_proposal_partition(self._session, proposal)

        stmt = pg_insert(RunInfo.__table__).values(
            proposal=proposal, run=run,
            start_time=start_time, added_at=added_at,
        ).on_conflict_do_nothing(index_elements=["proposal", "run"])
        self._session.execute(stmt)

        if start_time is not None:
            self._session.execute(
                RunInfo.__table__.update()
                .where(RunInfo.proposal == proposal)
                .where(RunInfo.run == run)
                .values(start_time=start_time)
            )
        self._session.commit()

    def change_run_comment(self, proposal: int, run: int, comment: str) -> None:
        self.set_variable(proposal, run, "comment", ReducedData(comment),
                          provenance="")

    def list_runs(self):
        """Return [(proposal, run, start_time, added_at)] ordered by proposal, run."""
        if self._proposal is None:
            stmt = select(RunInfo.proposal, RunInfo.run,
                          RunInfo.start_time, RunInfo.added_at)
        else:
            stmt = (
                select(RunInfo.proposal, RunInfo.run,
                       RunInfo.start_time, RunInfo.added_at)
                .where(RunInfo.proposal == self._proposal)
            )
        stmt = stmt.order_by(RunInfo.proposal, RunInfo.run)
        return [tuple(r) for r in self._session.execute(stmt).all()]

    def count_runs(self) -> int:
        stmt = select(func.count()).select_from(RunInfo)
        if self._proposal is not None:
            stmt = stmt.where(RunInfo.proposal == self._proposal)
        return int(self._session.scalar(stmt) or 0)

    # ---------- variables (computed + user) ----------
    def add_user_variable(self, variable: UserEditableVariable,
                          exist_ok: bool = False) -> None:
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")
        values = dict(
            proposal=self._proposal,
            name=variable.name,
            type=variable.variable_type,
            title=variable.title,
            description=variable.description,
        )
        stmt = pg_insert(Variable.__table__).values(**values)
        if exist_ok:
            stmt = stmt.on_conflict_do_update(
                index_elements=["proposal", "name"],
                set_={
                    "type": stmt.excluded.type,
                    "title": stmt.excluded.title,
                    "description": stmt.excluded.description,
                },
            )
        self._session.execute(stmt)
        self._session.commit()

    def get_user_variables(self) -> dict[str, UserEditableVariable]:
        if self._proposal is None:
            return {}
        stmt = (
            select(Variable.name, Variable.title, Variable.type,
                   Variable.description, Variable.attributes)
            .where(Variable.proposal == self._proposal)
            .where(Variable.type.isnot(None))
        )
        result = {}
        for name, title, type_, description, attrs in self._session.execute(stmt):
            result[name] = UserEditableVariable(
                name=name,
                title=title,
                variable_type=type_,
                description=description,
                attributes=json.dumps(attrs) if isinstance(attrs, dict) else attrs,
            )
        log.debug("Loaded %d user variables", len(result))
        return result

    def update_computed_variables(self, vars: dict) -> dict:
        """Upsert computed variable metadata and sync tags.

        Returns the dictionary of changed entries.
        """
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")

        existing_stmt = (
            select(Variable.name, Variable.title, Variable.type,
                   Variable.description, Variable.attributes, Variable.tags)
            .where(Variable.proposal == self._proposal)
            .where(Variable.type.is_(None))
        )
        vars_in_db = {}
        for name, title, type_, description, attributes, tags in self._session.execute(existing_stmt):
            vars_in_db[name] = {
                "title": title,
                "description": description,
                "attributes": attributes,
                "tags": list(tags or []),
                "type": type_,
            }

        updates = {}
        for name, new in vars.items():
            # Normalise tag lists for comparison
            new_cmp = {
                "title": new.get("title"),
                "description": new.get("description"),
                "attributes": new.get("attributes"),
                "tags": sorted(new.get("tags") or []),
                "type": None,
            }
            old = vars_in_db.get(name)
            if old is None:
                updates[name] = new
                continue
            old_cmp = dict(old)
            old_cmp["tags"] = sorted(old_cmp.get("tags") or [])
            if new_cmp != old_cmp:
                updates[name] = new

        log.debug("Updating stored metadata for %d computed variables", len(updates))

        for name, new in updates.items():
            tags = list(new.get("tags") or [])
            attrs = new.get("attributes")
            payload = dict(
                proposal=self._proposal,
                name=name,
                type=None,
                title=new.get("title"),
                description=new.get("description"),
                attributes=attrs,
                tags=tags,
            )
            stmt = pg_insert(Variable.__table__).values(**payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=["proposal", "name"],
                set_={
                    "type": stmt.excluded.type,
                    "title": stmt.excluded.title,
                    "description": stmt.excluded.description,
                    "attributes": stmt.excluded.attributes,
                    "tags": stmt.excluded.tags,
                },
            )
            self._session.execute(stmt)

        self._session.commit()
        return updates

    def variable_names(self) -> list[str]:
        if self._proposal is None:
            return []

        rv_names = self._session.execute(
            select(RunVariable.name.distinct())
            .where(RunVariable.proposal == self._proposal)
        ).scalars().all()
        var_names = self._session.execute(
            select(Variable.name).where(Variable.proposal == self._proposal)
        ).scalars().all()
        return list(set(rv_names) | set(var_names))

    def variable_titles(self) -> dict[str, str]:
        """Return {name: title} for all variables with a non-null title."""
        if self._proposal is None:
            return {}
        stmt = (
            select(Variable.name, Variable.title)
            .where(Variable.proposal == self._proposal)
            .where(Variable.title.isnot(None))
        )
        return dict(self._session.execute(stmt).all())

    def set_variable(self, proposal: int, run: int, name: str,
                     reduced: ReducedData, provenance: str) -> None:
        ensure_proposal_partition(self._session, proposal)
        timestamp = datetime.now(tz=timezone.utc).timestamp()

        encoded = encode_value(reduced)
        attributes = encoded.attributes if isinstance(encoded.attributes, dict) else None

        # Retrieve the current version to increment.
        current = self._session.execute(
            select(RunVariable.version)
            .where(RunVariable.proposal == proposal)
            .where(RunVariable.run == run)
            .where(RunVariable.name == name)
            .where(RunVariable.is_current.is_(True))
        ).scalar_one_or_none()

        version = (current or 0) + 1

        # Flip the previous "current" row, if any.
        if current is not None:
            self._session.execute(
                RunVariable.__table__.update()
                .where(RunVariable.proposal == proposal)
                .where(RunVariable.run == run)
                .where(RunVariable.name == name)
                .where(RunVariable.is_current.is_(True))
                .values(is_current=False)
            )

        stmt = pg_insert(RunVariable.__table__).values(
            proposal=proposal, run=run, name=name, version=version,
            is_current=True,
            value_kind=encoded.kind.value,
            value_num=encoded.num,
            value_text=encoded.text,
            value_bool=encoded.bool_,
            value_bytes=encoded.bytes_,
            timestamp=timestamp,
            max_diff=encoded.max_diff,
            provenance=provenance,
            summary_method=encoded.summary_method or None,
            summary_type=encoded.summary_type,
            attributes=attributes,
        )
        self._session.execute(stmt)
        self._session.commit()

    def iter_run_variables(self, proposal: Optional[int] = None,
                           run: Optional[int] = None,
                           names: Optional[list[str]] = None,
                           current_only: bool = True):
        """Iterate rows (name, value, max_diff, summary_type, attributes, provenance).

        ``value`` is the decoded Python object.
        """
        stmt = select(
            RunVariable.proposal, RunVariable.run, RunVariable.name,
            RunVariable.value_kind, RunVariable.value_num, RunVariable.value_text,
            RunVariable.value_bool, RunVariable.value_bytes,
            RunVariable.max_diff, RunVariable.summary_type,
            RunVariable.attributes, RunVariable.provenance,
        )
        if current_only:
            stmt = stmt.where(RunVariable.is_current.is_(True))
        prop = proposal if proposal is not None else self._proposal
        if prop is not None:
            stmt = stmt.where(RunVariable.proposal == prop)
        if run is not None:
            stmt = stmt.where(RunVariable.run == run)
        if names:
            stmt = stmt.where(RunVariable.name.in_(tuple(names)))
        stmt = stmt.order_by(RunVariable.proposal, RunVariable.run)

        for row in self._session.execute(stmt):
            value = decode_value(
                row.value_kind,
                num=row.value_num, text=row.value_text,
                bool_=row.value_bool, bytes_=row.value_bytes,
                summary_type=row.summary_type,
            )
            yield (row.proposal, row.run, row.name, value,
                   row.max_diff, row.summary_type, row.attributes, row.provenance)

    def get_variable(self, proposal: int, run: int, name: str):
        """Return the current (value, summary_type, attributes) tuple or None."""
        stmt = (
            select(RunVariable.value_kind, RunVariable.value_num,
                   RunVariable.value_text, RunVariable.value_bool,
                   RunVariable.value_bytes, RunVariable.summary_type,
                   RunVariable.attributes)
            .where(RunVariable.proposal == proposal)
            .where(RunVariable.run == run)
            .where(RunVariable.name == name)
            .where(RunVariable.is_current.is_(True))
        )
        row = self._session.execute(stmt).first()
        if row is None:
            return None
        value = decode_value(
            row.value_kind, num=row.value_num, text=row.value_text,
            bool_=row.value_bool, bytes_=row.value_bytes,
            summary_type=row.summary_type,
        )
        return (value, row.summary_type, row.attributes)

    def delete_variable(self, name: str) -> None:
        if self._proposal is None:
            raise ValueError("DamnitDB has no active proposal")
        self._session.execute(
            delete(RunVariable).where(RunVariable.proposal == self._proposal)
                               .where(RunVariable.name == name)
        )
        self._session.execute(
            delete(Variable).where(Variable.proposal == self._proposal)
                            .where(Variable.name == name)
        )
        self._session.commit()

    # ---------- tags ----------
    def _set_variable_tags(self, variable_name: str, tags: list[str]) -> None:
        self._session.execute(
            Variable.__table__.update()
            .where(Variable.proposal == self._proposal)
            .where(Variable.name == variable_name)
            .values(tags=list(tags))
        )

    def get_variable_tags(self, variable_name: str) -> list[str]:
        if self._proposal is None:
            return []
        row = self._session.execute(
            select(Variable.tags)
            .where(Variable.proposal == self._proposal)
            .where(Variable.name == variable_name)
        ).first()
        if row is None or row[0] is None:
            return []
        return list(row[0])

    def tag_variable(self, variable_name: str, tag_name: str) -> None:
        current = set(self.get_variable_tags(variable_name))
        if tag_name in current:
            return
        current.add(tag_name)
        self._set_variable_tags(variable_name, sorted(current))
        self._session.commit()

    def untag_variable(self, variable_name: str, tag_name: str) -> None:
        current = set(self.get_variable_tags(variable_name))
        if tag_name not in current:
            return
        current.discard(tag_name)
        self._set_variable_tags(variable_name, sorted(current))
        self._session.commit()

    def add_tag(self, tag_name: str) -> str:
        """Present for backwards compatibility. Returns the tag name itself."""
        return tag_name

    def get_tag_id(self, tag_name: str) -> Optional[str]:
        if self._proposal is None:
            return None
        for existing in self.get_all_tags():
            if existing == tag_name:
                return tag_name
        return None

    def get_variables_by_tag(self, tag_name: str) -> list[str]:
        if self._proposal is None:
            return []
        stmt = (
            select(Variable.name)
            .where(Variable.proposal == self._proposal)
            .where(Variable.tags.any(tag_name))
        )
        return list(self._session.execute(stmt).scalars().all())

    def get_all_tags(self) -> list[str]:
        if self._proposal is None:
            return []
        # Gather all tags by unnesting the array column.
        stmt = text(
            "SELECT DISTINCT UNNEST(tags) AS tag "
            "FROM variables WHERE proposal = :p ORDER BY tag"
        )
        return [row[0] for row in self._session.execute(stmt, {"p": self._proposal})]

    # ---------- notification helpers ----------
    def send_message(self, kind: MsgKind, data: dict) -> None:
        notify(self._session, self.notify_channel, kind, data)
        self._session.commit()

    def run_values_updated(self, proposal: int, run: int, names: list[str]) -> None:
        self.send_message(MsgKind.run_values_updated, {
            "proposal": proposal,
            "run": run,
            "values": {name: None for name in names},
        })

    def variable_set(self, name: str, title: str, description: str,
                     variable_type: Optional[str]) -> None:
        self.send_message(MsgKind.variable_set, {
            "name": name, "title": title,
            "description": description,
            "attributes": None,
            "type": variable_type,
        })

    def processing_state_set(self, info: dict) -> None:
        self.send_message(MsgKind.processing_state_set, info)

    def processing_finished(self, info: dict) -> None:
        self.send_message(MsgKind.processing_finished, info)


# ---------- proposal bootstrap ----------
def initialize_proposal(root_path, proposal=None, context_file_src=None,
                        user_vars_src=None):
    """Initialise a proposal directory + database entry.

    The directory is still created locally (for the context file,
    extracted_data subdirectory, supervisord, etc.), but proposal-level data
    is recorded in the shared Postgres database.
    """
    root_path = Path(root_path)
    root_path.mkdir(parents=True, exist_ok=True)
    if root_path.stat().st_uid == os.getuid():
        os.chmod(root_path, 0o777)

    # If no proposal was supplied, look for one already registered under
    # this directory via the listener metadata table. Otherwise require one.
    if proposal is None:
        raise ValueError(
            "Must pass a proposal number to initialize_proposal()."
        )

    new_db = False
    db = DamnitDB(proposal=proposal)
    try:
        info = db.proposal_settings()
        new_db = not info
        if new_db:
            db.set_setting("context_python", DEFAULT_CONTEXT_PYTHON)
    finally:
        db.close()

    context_path = root_path / "context.py"
    if not context_path.is_file():
        if context_file_src is not None:
            shutil.copyfile(context_file_src, context_path)
        else:
            context_path.touch()
        os.chmod(context_path, 0o666)

    if new_db and (user_vars_src is not None):
        prev_db = DamnitDB(proposal=int(user_vars_src))
        try:
            for var in prev_db.get_user_variables().values():
                DamnitDB(proposal=proposal).add_user_variable(var, exist_ok=True)
        finally:
            prev_db.close()

    return proposal


# Preserve a couple of names exported from the previous module.
__all__ = [
    "DamnitDB", "ReducedData", "MsgKind", "msg_dict",
    "BlobTypes", "blob2complex", "blob2numpy", "complex2blob", "numpy2blob",
    "ValueKind", "encode_value", "decode_value",
    "initialize_proposal",
]
