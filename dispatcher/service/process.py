import asyncio
import multiprocessing
from multiprocessing.context import BaseContext
from types import ModuleType
from typing import Callable, Iterable, Optional, Union

from ..worker.task import work_loop
from ..config import LazySettings
from ..config import settings as global_settings

# def work_loop(worker_id: int, queue: multiprocessing.Queue, settings: dict, finished_queue: multiprocessing.Queue) -> None:

class ProcessProxy:
    def __init__(
        self, args: Iterable, target: Callable = work_loop, ctx: Union[BaseContext, ModuleType] = multiprocessing
    ) -> None:
        self.message_queue: multiprocessing.Queue = ctx.Queue()
        # This is intended use of multiprocessing context, but not available on BaseContext
        self._process = ctx.Process(target=target, args=tuple(args) + (self.message_queue,))  # type: ignore

    def start(self) -> None:
        self._process.start()

    def join(self, timeout: Optional[int] = None) -> None:
        if timeout:
            self._process.join(timeout=timeout)
        else:
            self._process.join()

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid

    def exitcode(self) -> Optional[int]:
        return self._process.exitcode

    def is_alive(self) -> bool:
        return self._process.is_alive()

    def kill(self) -> None:
        self._process.kill()

    def terminate(self) -> None:
        self._process.terminate()


class ProcessManager:
    mp_context = 'fork'

    def __init__(self, settings: LazySettings = global_settings) -> None:
        self.ctx = multiprocessing.get_context(self.mp_context)
        self.finished_queue: multiprocessing.Queue = self.ctx.Queue()
        self.settings_stash: dict = settings.serialize()  # These are passed to the workers to initialize dispatcher settings
        self._loop = None

    def get_event_loop(self):
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        return self._loop

    def create_process(self, args: Iterable[int | str | dict], **kwargs) -> ProcessProxy:
        return ProcessProxy(args + (self.settings_stash, self.finished_queue), ctx=self.ctx, **kwargs)

    async def read_finished(self) -> dict[str, Union[str, int]]:
        message = await self.get_event_loop().run_in_executor(None, self.finished_queue.get)
        return message


class ForkServerManager(ProcessManager):
    mp_context = 'forkserver'

    def __init__(self, preload_modules: Optional[list[str]] = None, settings: LazySettings = global_settings):
        super().__init__(settings=settings)
        self.ctx.set_forkserver_preload(preload_modules if preload_modules else [])
