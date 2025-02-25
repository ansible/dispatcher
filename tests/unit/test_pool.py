import asyncio

import pytest

from dispatcher.factories import from_settings
from dispatcher.config import temporary_settings


@pytest.mark.asyncio
async def test_no_op_task():
    with temporary_settings({'version': 2}):
        dispatcher = from_settings()
        pool = dispatcher.pool
        await pool.start_working(dispatcher)
        cleared_task = asyncio.create_task(pool.events.work_cleared.wait())
        await pool.dispatch_task({'task': 'lambda: None'})
        await cleared_task
        await pool.shutdown()
