import asyncio

import pytest

from dispatcher.pool import WorkerPool
from dispatcher.main import DispatcherMain


@pytest.mark.asyncio
async def test_no_op_task():
    pool = WorkerPool(1)
    await pool.start_working(DispatcherMain({}))
    cleared_task = asyncio.create_task(pool.events.work_cleared.wait())
    await pool.dispatch_task({'task': 'lambda: None'})
    await cleared_task
    await pool.shutdown()
