import getpass
import json
import logging
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from socket import gethostname
from threading import Thread

from kafka import KafkaConsumer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..api import find_proposal
from ..context import RunData
from ..definitions import DEFAULT_DAMNIT_PYTHON
from .db import DamnitDB, get_sessionmaker
from .db.models import ListenerProposal, ListenerSetting
from .extraction_control import ExtractionRequest, ExtractionSubmitter
from .service import notify_ready

# Migration & calibration events come via DESY's Kafka brokers. DAMNIT
# uses Postgres LISTEN/NOTIFY internally, but consuming DESY events still
# requires a Kafka consumer.
CONSUMER_ID = 'xfel-da-damnit-{}'
KAFKA_CONF = {
    'maxwell': {
        'brokers': ['exflwgs06:9091'],
        'topics': ["test.r2d2", "cal.offline-corrections"],
        'events': ["migration_complete", "run_corrections_complete"],
    },
    'onc': {
        'brokers': ['exflwgs06:9091'],
        'topics': ['test.euxfel.hed.daq', 'test.euxfel.hed.cal'],
        'events': ['daq_run_complete', 'online_correction_complete'],
    }
}

log = logging.getLogger(__name__)

# Tracking number of local threads running in parallel; only relevant if
# slurm isn't available.
MAX_CONCURRENT_THREADS = min(os.cpu_count() // 2, 10)
local_extraction_threads = []


@dataclass
class ProposalDBInfo:
    db_dir: Path
    official: bool


def execute_direct(submitter, request):
    for th in local_extraction_threads.copy():
        if not th.is_alive():
            local_extraction_threads.pop(local_extraction_threads.index(th))

    if len(local_extraction_threads) >= MAX_CONCURRENT_THREADS:
        log.warning(f'Too many events processing ({MAX_CONCURRENT_THREADS}), '
                    f'skip event (p{request.proposal}, r{request.run}, {request.run_data.value})')
        return

    def _run():
        try:
            submitter.execute_direct(request)
        except Exception:
            log.error(f"Local extraction of p{request.proposal}, r{request.run} failed:", exc_info=True)

    extr = Thread(target=_run)
    local_extraction_threads.append(extr)
    extr.start()


class ListenerDB:
    """Listener state (subscribed proposals + settings) stored in Postgres."""

    def __init__(self, db_dir: Path = None):
        # ``db_dir`` is kept in the signature for CLI compatibility but is
        # no longer used; state lives in the shared Postgres database.
        self._sm = get_sessionmaker()
        self._session = self._sm()
        # Seed defaults so the setter never races with first-time lookups.
        self.settings.setdefault("static_mode", "true")
        self.settings.setdefault("allow_local_processing", "false")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._session.close()

    @property
    def settings(self) -> "ListenerSettings":
        return ListenerSettings(self._session)

    def all_proposals(self) -> dict[int, list[ProposalDBInfo]]:
        rows = self._session.execute(
            select(ListenerProposal.proposal, ListenerProposal.db_dir,
                   ListenerProposal.official)
        ).all()
        result: dict[int, list[ProposalDBInfo]] = {}
        for proposal, db_dir, official in rows:
            result.setdefault(proposal, []).append(
                ProposalDBInfo(Path(db_dir), bool(official))
            )
        return result

    def proposal_db_dirs(self, proposal: int) -> list[Path]:
        stmt = (
            select(ListenerProposal.db_dir)
            .where(ListenerProposal.proposal == proposal)
        )
        return [Path(row[0]) for row in self._session.execute(stmt)]

    def add_proposal_db(self, proposal: int, db_dir, official: bool) -> None:
        stmt = pg_insert(ListenerProposal.__table__).values(
            proposal=int(proposal), db_dir=str(db_dir), official=bool(official),
        ).on_conflict_do_update(
            index_elements=["proposal"],
            set_={"db_dir": str(db_dir), "official": bool(official)},
        )
        self._session.execute(stmt)
        self._session.commit()

    def remove_proposal_db(self, db_dir) -> None:
        self._session.execute(
            ListenerProposal.__table__.delete()
            .where(ListenerProposal.db_dir == str(db_dir))
        )
        self._session.commit()


class ListenerSettings:
    """MutableMapping-ish helper backed by the listener_settings table."""

    _BOOL_KEYS = {"static_mode", "allow_local_processing"}

    def __init__(self, session):
        self._s = session

    def _normalise(self, key: str, value):
        if value is None:
            return None
        if key in self._BOOL_KEYS:
            if isinstance(value, bool):
                return "true" if value else "false"
            return str(value).lower()
        return str(value)

    def _cast(self, key: str, value):
        if value is None:
            return None
        if key in self._BOOL_KEYS:
            return value.lower() in ("1", "true", "yes", "on")
        return value

    def __getitem__(self, key):
        row = self._s.get(ListenerSetting, key)
        if row is None:
            raise KeyError(key)
        return self._cast(key, row.value)

    def __setitem__(self, key, value):
        stmt = pg_insert(ListenerSetting.__table__).values(
            key=key, value=self._normalise(key, value),
        ).on_conflict_do_update(
            index_elements=["key"],
            set_={"value": self._normalise(key, value)},
        )
        self._s.execute(stmt)
        self._s.commit()

    def __delitem__(self, key):
        self._s.execute(
            ListenerSetting.__table__.delete()
            .where(ListenerSetting.key == key)
        )
        self._s.commit()

    def __iter__(self):
        return iter(
            row[0] for row in self._s.execute(select(ListenerSetting.key))
        )

    def __len__(self):
        from sqlalchemy import func
        return int(self._s.scalar(
            select(func.count()).select_from(ListenerSetting)
        ) or 0)

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return self._cast(key, self._normalise(key, default))

    def items(self):
        stmt = select(ListenerSetting.key, ListenerSetting.value)
        return [(k, self._cast(k, v)) for k, v in self._s.execute(stmt)]


class EventProcessor:
    def __init__(self, listener_dir: Path):
        self._listener_dir = listener_dir
        self.db = ListenerDB(listener_dir)

        hostname = gethostname()
        if hostname.startswith('exflonc'):
            kafka_conf = KAFKA_CONF['onc']
        else:
            kafka_conf = KAFKA_CONF['maxwell']

        group_id = CONSUMER_ID.format(str(listener_dir).replace("/", "_"))
        client_id = CONSUMER_ID.format(f"{hostname}-{os.getpid()}")
        self.kafka_cns = KafkaConsumer(*kafka_conf['topics'],
                                       bootstrap_servers=kafka_conf['brokers'],
                                       group_id=group_id,
                                       client_id=client_id,
                                       consumer_timeout_ms=600_000,
                                       )
        self.events = kafka_conf['events']
        log.info("Started listener")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kafka_cns.close()
        self.db.close()
        return False

    def run(self):
        while True:
            for record in self.kafka_cns:
                try:
                    self._process_kafka_event(record)
                except Exception:
                    log.error("Unexpected error handling Kafka event.", exc_info=True)

    def _process_kafka_event(self, record):
        msg = json.loads(record.value.decode())
        event = msg.get('event')
        if event in self.events:
            log.debug("Processing %s event from Kafka", event)
            getattr(self, f'handle_{event}')(record, msg)
        else:
            log.debug("Unexpected %s event from Kafka", event)

    def handle_daq_run_complete(self, record, msg: dict):
        self.handle_event(record, msg, RunData.RAW)

    def handle_online_correction_complete(self, record, msg: dict):
        self.handle_event(record, msg, RunData.PROC)

    def handle_migration_complete(self, record, msg: dict):
        self.handle_event(record, msg, RunData.RAW)

    def handle_run_corrections_complete(self, record, msg: dict):
        self.handle_event(record, msg, RunData.PROC)

    def handle_event(self, record, msg: dict, run_data: RunData):
        proposal = int(msg['proposal'])
        run = int(msg['run'])

        try:
            official_path = find_proposal(proposal) / "usr/Shared/amore"
        except FileNotFoundError:
            log.warning(f"Could not find proposal directory for p{proposal}")
            official_path = None

        static_mode = self.db.settings.get("static_mode", True)
        if (official_path and not static_mode
                and official_path not in self.db.proposal_db_dirs(proposal)):
            self.db.add_proposal_db(proposal, official_path, True)

        sandbox_args = self.db.settings.get("sandbox_args", "") or ""
        allow_local_processing = bool(self.db.settings.get("allow_local_processing", False))
        for path in self.db.proposal_db_dirs(proposal):
            try:
                with DamnitDB(proposal=proposal) as db:
                    db.ensure_run(proposal, run, record.timestamp / 1000)
                    log.info(f"Added p%d r%d ({run_data.value} data) to database",
                             proposal, run)

                    damnit_python = db.get_setting("damnit_python") or DEFAULT_DAMNIT_PYTHON
                    if db.get_setting("damnit_python") is None:
                        db.set_setting("damnit_python", damnit_python)
                    submitter = ExtractionSubmitter(path, db)
                    req = ExtractionRequest(run, proposal, run_data,
                                            sandbox_args, damnit_python)

                try:
                    submitter.submit(req)
                except Exception as e:
                    if allow_local_processing:
                        log.error("Slurm job submission failed, starting process locally.",
                                  exc_info=True)
                        execute_direct(submitter, req)
                    else:
                        raise e
            except Exception:
                log.error(f"Processing p{proposal}, r{run} for {path} failed:",
                          exc_info=True)


def listen(db_dir):
    file_handler = logging.FileHandler("damnit.log")
    formatter = logging.root.handlers[0].formatter
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)

    log.info(f"Running on {platform.node()} under user {getpass.getuser()}, PID {os.getpid()}")
    notify_ready()
    try:
        with EventProcessor(db_dir) as processor:
            processor.run()
    except KeyboardInterrupt:
        log.error("Stopping on Ctrl + C")
    except Exception:
        log.error("Stopping on unexpected error", exc_info=True)

    logging.shutdown()


if __name__ == '__main__':
    listen(Path.cwd())
