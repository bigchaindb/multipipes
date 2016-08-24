import pytest

import time
from multipipes import utils


def test_deadline_doesnt_raise_if_timeout_is_not_reached():
    called = False

    with utils.deadline(timeout=0.1):
        called = True

    time.sleep(0.5)
    assert called is True


def test_deadline_doesnt_run_callback_if_timeout_not_reached():
    called = False

    with pytest.raises(TimeoutError):
        with utils.deadline(timeout=0.1):
            time.sleep(0.5)
            called = True

    assert called is False
