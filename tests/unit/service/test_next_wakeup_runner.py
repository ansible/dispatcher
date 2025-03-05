import time
from unittest import mock
import asyncio

import pytest

from dispatcher.service.next_wakeup_runner import NextWakeupRunner, HasWakeup


class ObjectWithWakeup(HasWakeup):
    def __init__(self, period):
        self.period = period
        self.last_run = time.monotonic()

    def next_wakeup(self):
        if self.period is None:
            return None
        return self.last_run + self.period


def test_get_next_wakeup():
    objects = set()
    obj = ObjectWithWakeup(1)
    objects.add(obj)
    callback = mock.MagicMock()
    runner = NextWakeupRunner(objects, callback)
    assert runner.get_next_wakeup() > time.monotonic()
    assert runner.get_next_wakeup() < time.monotonic() + 1.

    obj.last_run = time.monotonic() + 0.1
    assert runner.get_next_wakeup() > time.monotonic() + 1.

    obj.period = None
    assert runner.get_next_wakeup() is None

    callback.assert_not_called()


@pytest.mark.asyncio
async def test_run_and_exit_task():
    objects = set()
    obj = ObjectWithWakeup(0.01)  # runs in 0.01 seconds, test will take this long
    objects.add(obj)

    async def callback_makes_done():
        obj.period = None  # No need to run ever again
        callback_makes_done.is_called = True

    runner = NextWakeupRunner(objects, callback_makes_done)

    await runner.background_task()  # should finish

    assert callback_makes_done.is_called is True


@pytest.mark.asyncio
async def test_graceful_shutdown():
    objects = set()
    obj = ObjectWithWakeup(1)
    obj.last_run -= 1.0  # make first run immediate
    objects.add(obj)
    callback = mock.MagicMock()

    async def mock_process_tasks():
        obj.last_run = time.monotonic()  # did whatever we do with the things
        callback()  # track for assertion

    runner = NextWakeupRunner(objects, mock_process_tasks)
    runner.kick()  # creates task, starts running

    # Poll for the objects data to reflect that it has been processed
    for _ in range(10):
        await asyncio.sleep(0.01)
        if obj.last_run >= time.monotonic() - 1.0:
            break
    else:
        raise RuntimeError('Object was never marked as ran as expected')


    runner.shutting_down = True
    runner.kick()
    await runner.asyncio_task
    assert runner.asyncio_task.done()

    callback.assert_called_once_with()

    assert runner.asyncio_task.done() is True
