import asyncio
import logging
import time
from abc import abstractmethod
from typing import Any, Callable, Coroutine, Iterable, Optional

logger = logging.getLogger(__name__)


class HasWakeup:
    """A mixin to indicate that this class gives a future timestamp of when a call is needed"""

    @abstractmethod
    def next_wakeup(self) -> Optional[float]:
        """The next time that we need to call the callback for, outline of contract:

        return None - no need to call the callback
        return positive value - however long we need to wait before calling the callback
        return zero of negative value - callback needs to be called right away
        """
        ...


def ensure_fatal(task: asyncio.Task) -> None:
    "General utility to make sure errors are raised"
    try:
        task.result()
    except asyncio.CancelledError:
        logger.info(f'Ack that task {task} was canceled')


class NextWakeupRunner:
    """Implements a general contract to wakeup for next timestamp of a set of objects

    For example, you have a set of schedules, each with a given period.
    You want to run each on their period - this does that using one lazy asyncio task.
    This is a repeated pattern in the code base with task schedules, delays, and timeouts
    """
    def __init__(self, wakeup_objects: Iterable[HasWakeup], callback: Callable[[], Coroutine[Any, Any, None]], name: Optional[str] = None) -> None:
        self.wakeup_objects = wakeup_objects
        self.callback = callback
        self.asyncio_task: Optional[asyncio.Task] = None
        self.kick_event = asyncio.Event()
        self.shutting_down: bool = False
        if name is None:
            self.name = f'next-run-manager-of-{wakeup_objects}'
        else:
            self.name = name

    def get_next_wakeup(self) -> Optional[float]:
        """Get the soonest next-run time. A None value indicates no next-run"""
        next_wakeup = None
        for obj in self.wakeup_objects:
            nr_item = obj.next_wakeup()
            if nr_item is None:
                continue
            if next_wakeup is None:
                next_wakeup = nr_item
            elif nr_item < next_wakeup:
                next_wakeup = nr_item
        return next_wakeup

    async def background_task(self) -> None:
        while not self.shutting_down:
            next_wakeup = self.get_next_wakeup()
            if next_wakeup is None:
                return

            now_time = time.monotonic()
            if next_wakeup <= now_time:
                await self.callback()
                next_wakeup = self.get_next_wakeup()
                if next_wakeup is None:
                    return

            delta = next_wakeup - now_time
            if delta >= 0.0:
                try:
                    await asyncio.wait_for(self.kick_event.wait(), timeout=delta)
                except asyncio.TimeoutError:
                    pass  # intended mechanism to hit the next schedule
                except asyncio.CancelledError:
                    logger.info(f'Task {self.name} cancelled, returning')
                    return

            self.kick_event.clear()

    def mk_new_task(self) -> None:
        """Should only be called if a task is not currently running"""
        self.asyncio_task = asyncio.create_task(self.background_task(), name=self.name)
        self.asyncio_task.add_done_callback(ensure_fatal)

    def kick(self) -> None:
        """Initiates the asyncio task to wake up at the next run time

        This needs to be called if objects in wakeup_objects are changed, for example
        """
        if self.get_next_wakeup() is None:
            # Optimization here, if there is no next time, do not bother managing tasks
            return
        if self.asyncio_task:
            if self.asyncio_task.done():
                self.mk_new_task()
            else:
                self.kick_event.set()
        else:
            self.mk_new_task()
