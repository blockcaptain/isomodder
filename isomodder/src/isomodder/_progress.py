import io
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Iterable, Callable, cast

SIZE_256KiB = 256 * 1024


def chunk_stream(stream: io.RawIOBase) -> Iterable[bytes]:
    return iter(partial(stream.read, SIZE_256KiB), b"")


class ProgressReporter(ABC):
    @abstractmethod
    def start_task(self, task_id: Any) -> None:
        ...

    @abstractmethod
    def stop_task(self, task_id: Any) -> None:
        ...

    @abstractmethod
    def update(
        self, task_id: Any, total: float = None, completed: float = None, advance: float = None,
    ) -> None:
        ...

    @abstractmethod
    def add_task(self, description: str, start: bool = True, total: float = 100,) -> Any:
        ...
