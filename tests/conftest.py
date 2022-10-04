import textwrap
from unittest.mock import MagicMock

import pytest
import numpy as np

from amore_mid_prototype.context import ContextFile
from amore_mid_prototype.backend.db import open_db, DB_NAME


@pytest.fixture
def mock_ctx():
    code = """
    import time
    import numpy as np
    from amore_mid_prototype.context import Variable

    @Variable(title="Scalar1")
    def scalar1(run):
        return 42

    @Variable(title="Scalar2")
    def scalar2(run, foo: "var#scalar1"):
        return 3.14

    @Variable(title="Array")
    def array(run, foo: "var#scalar1", bar: "var#scalar2"):
        return np.array([foo, bar])

    @Variable(title="Timestamp")
    def timestamp(run):
        return time.time()

    @Variable(data="proc")
    def meta_array(run, baz: "var#array", run_number: "meta#run_number",
                   proposal: "meta#proposal", ts: "var#timestamp"):
        return np.array([run_number, proposal])

    @Variable(data="raw")
    def string(run, proposal_path: "meta#proposal_path"):
        return str(proposal_path)
    """

    return ContextFile.from_str(textwrap.dedent(code))

@pytest.fixture
def mock_run():
    run = MagicMock()

    run.train_ids = np.arange(10)

    def select_trains(train_slice):
        return run

    run.select_trains.side_effect = select_trains

    def train_timestamps():
        return np.array(run.train_ids + 1493892000000000000,
                        dtype="datetime64[ns]")

    run.train_timestamps.side_effect = train_timestamps

    run.files = [MagicMock(filename="/tmp/foo/bar.h5")]

    return run

@pytest.fixture
def mock_db(tmp_path):
    db = open_db(tmp_path / DB_NAME)

    yield tmp_path, db

    db.close()
