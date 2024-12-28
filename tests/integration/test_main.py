import asyncio
import json

import pytest

from tests.data import methods as test_methods

SLEEP_METHOD = 'lambda: __import__("time").sleep(0.1)'


@pytest.mark.asyncio
async def test_run_lambda_function(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await pg_message('lambda: "This worked!"')
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_run_decorated_function(apg_dispatcher, conn_config):
    assert apg_dispatcher.pool.finished_count == 0

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    test_methods.print_hello.apply_async(config=conn_config)
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_multiple_channels(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.gather(
        pg_message(SLEEP_METHOD, channel='test_channel'),
        pg_message(SLEEP_METHOD, channel='test_channel2'),
        pg_message(SLEEP_METHOD, channel='test_channel3'),
        pg_message(SLEEP_METHOD, channel='test_channel4')  # not listening to this
    )
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 3


@pytest.mark.asyncio
async def test_ten_messages_queued(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.gather(*[pg_message(SLEEP_METHOD) for i in range(15)])
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 15


@pytest.mark.asyncio
async def test_get_running_jobs(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_me'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    worker_id, running_job = running_jobs[0][0]

    assert running_job['uuid'] == 'find_me'


@pytest.mark.asyncio
async def test_cancel_task(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'foobar'}, timeout=1), timeout=5)
    worker_id, canceled_message = canceled_jobs[0][0]
    assert canceled_message['uuid'] == 'foobar'
    await asyncio.wait_for(clearing_task, timeout=3)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [0, 1, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_message_with_delay(apg_dispatcher, pg_message, pg_control):
    assert apg_dispatcher.pool.finished_count == 0

    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task had a delay")', 'uuid': 'delay_task', 'delay': 0.2})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    worker_id, running_job = running_jobs[0][0]
    assert worker_id == '<delayed>'
    assert running_job['uuid'] == 'delay_task'
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [0, 0, 1], 'cts: [finished, canceled, control]'
    # Completing the reply itself will be a work_cleared event, so we have to clear the event
    apg_dispatcher.pool.events.work_cleared.clear()

    # Wait for task to finish, assertions after completion
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [1, 0, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_cancel_delayed_task(apg_dispatcher, pg_message, pg_control):
    assert apg_dispatcher.pool.finished_count == 0

    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 0.8})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'delay_task_will_cancel'}, timeout=1), timeout=5)
    worker_id, canceled_job = canceled_jobs[0][0]
    assert worker_id == '<delayed>'
    assert canceled_job['uuid'] == 'delay_task_will_cancel'

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert running_jobs == [[]]

    assert apg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_cancel_with_no_reply(apg_dispatcher, pg_message, pg_control):
    assert apg_dispatcher.pool.finished_count == 0

    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 2.0})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    await pg_control.acontrol('cancel', data={'uuid': 'delay_task_will_cancel'})
    await asyncio.sleep(0.04)

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert running_jobs == [[]]

    assert apg_dispatcher.pool.finished_count == 0

    # print('')
    # print('running alive check a few times')
    # for i in range(3):
    #     alive = ctl.control_with_reply('alive')
    #     print(alive)

    # print('')
    # print('demo of submitting discarding tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}
    #     ), config={'conninfo': CONNECTION_STRING})
    # print('demo of discarding task marked as discarding')
    # for i in range(10):
    #     sleep_discard.apply_async(args=[2], config={'conninfo': CONNECTION_STRING})
    # print('demo of discarding tasks with apply_async contract')
    # for i in range(10):
    #     sleep_function.apply_async(args=[3], on_duplicate='discard', config={'conninfo': CONNECTION_STRING})
    # print('demo of submitting waiting tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(10)', 'on_duplicate': 'serial', 'uuid': f'wait-{i}'}
    #         ), config={'conninfo': CONNECTION_STRING})
    # print('demo of submitting queue-once tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(8)', 'on_duplicate': 'queue_one', 'uuid': f'queue_one-{i}'}
    #     ), config={'conninfo': CONNECTION_STRING})