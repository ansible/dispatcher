import asyncio

import pytest

from dispatcher.brokers.pg_notify import create_connection
from dispatcher.factories import get_control_from_settings


@pytest.mark.benchmark(group="control")
def test_alive_benchmark(benchmark, with_full_server, test_settings):
    control = get_control_from_settings(settings=test_settings)

    def alive_check():
        r = control.control_with_reply('alive')
        assert r == [None]

    with with_full_server(4):
        benchmark(alive_check)


@pytest.mark.benchmark(group="control")
@pytest.mark.parametrize('messages', [0, 3, 4, 5, 10, 100])
def test_alive_benchmark_while_busy(benchmark, with_full_server, test_settings, messages):
    control = get_control_from_settings(settings=test_settings)

    def alive_check():
        submit_conn = create_connection(**test_settings.brokers['pg_notify']['config'])
        function = 'lambda: __import__("time").sleep(0.01)'
        with submit_conn.cursor() as cur:
            for i in range(messages):
                cur.execute(f"SELECT pg_notify('test_channel', '{function}');")
        r = control.control_with_reply('alive', timeout=2)
        assert r == [None]

    with with_full_server(4):
        benchmark(alive_check)
