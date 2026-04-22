"""GUI-side Postgres LISTEN/NOTIFY agent.

Kept at this path (``damnit.gui.kafka``) for backwards compatibility with
existing imports. The underlying transport is now Postgres LISTEN/NOTIFY.
"""
import logging

from PyQt5 import QtCore

from ..backend.db import MsgKind, PgListener, channel_for, msg_dict

log = logging.getLogger(__name__)


class UpdateAgent(QtCore.QObject):
    message = QtCore.pyqtSignal(object)

    def __init__(self, db_id: str) -> None:
        QtCore.QObject.__init__(self)
        self._channel = channel_for(db_id)
        self._listener = PgListener(self._channel)
        self.running = False

    @property
    def update_topic(self) -> str:
        """Kept for API parity with the previous Kafka-based agent."""
        return self._channel

    def listen_loop(self) -> None:
        self.running = True
        try:
            for kind, data in self._listener.iter_notifies():
                if not self.running:
                    break
                if kind is None:
                    continue
                self.message.emit(msg_dict(MsgKind(kind), data))
        except Exception:
            log.exception("PgListener loop died unexpectedly")

    def run_values_updated(self, proposal, run, name):
        # Emitting notifications is the backend's responsibility now - but we
        # still surface a matching signal locally so that GUI code receives
        # confirmation when we write from the GUI itself.
        message = msg_dict(MsgKind.run_values_updated, {
            "proposal": proposal,
            "run": run,
            "values": {name: None},
        })
        self.message.emit(message)

    def variable_set(self, name, title, description, variable_type):
        message = msg_dict(MsgKind.variable_set, {
            "name": name,
            "title": title,
            "attributes": None,
            "type": variable_type,
        })
        self.message.emit(message)

    def processing_submitted(self, info):
        self.message.emit(msg_dict(MsgKind.processing_state_set, info))

    def stop(self):
        self.running = False
        self._listener.stop()
