import asyncio

import pytest

from dispatcher.control import Control
from dispatcher.brokers.pg_notify import get_connection


@pytest.mark.benchmark(group="control")
def test_alive_benchmark(benchmark, with_full_server, conn_config):
    control = Control('test_channel', config=conn_config)

    def alive_check():
        r = control.control_with_reply('alive')
        assert r == [None]

    with with_full_server(4):
        benchmark(alive_check)


@pytest.mark.benchmark(group="control")
@pytest.mark.parametrize('messages', [0, 3, 4, 5, 10, 100])
def test_alive_benchmark_while_busy(benchmark, with_full_server, conn_config, messages):
    control = Control('test_channel', config=conn_config)

    def alive_check():
        submit_conn = get_connection(conn_config)
        function = 'lambda: __import__("time").sleep(0.01)'
        with submit_conn.cursor() as cur:
            for i in range(messages):
                cur.execute(f"SELECT pg_notify('test_channel', '{function}');")
        r = control.control_with_reply('alive', timeout=2)
        assert r == [None]

    with with_full_server(4):
        benchmark(alive_check)
