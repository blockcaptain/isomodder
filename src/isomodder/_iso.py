import functools
import logging
import stat
import tempfile
from pathlib import Path
from types import TracebackType
from typing import IO, BinaryIO, Callable, ContextManager, Optional, Tuple, Type, cast

import pycdlib
from pycdlib.dr import DirectoryRecord
from pycdlib.facade import PyCdlibRockRidge
from pycdlib.rockridge import RockRidge

from ._iso_untyped import write_progress_callback
from ._progress import ProgressReporter, SIZE_256KiB


class IsoFile(ContextManager[None]):
    def __init__(self, path: Path, working_dir: Path = None):
        self._iso = pycdlib.PyCdlib()
        self._iso.open(str(path))
        self._iso_rr: PyCdlibRockRidge = self._iso.get_rock_ridge_facade()
        self._temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self._working_dir = working_dir

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self._iso.close()
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
        return None

    @property
    def _temp_path(self) -> Path:
        if self._temp_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory(dir=self._working_dir)
        return Path(self._temp_dir.name)

    def _cdlib_paths(self, path: Path, is_dir: bool) -> Tuple[str, str, str]:
        rr_path = make_rr_path(path)
        iso_path, rr_name = self._iso_rr._rr_path_to_iso_path_and_rr_name(rr_path, is_dir=is_dir)
        return (rr_path, iso_path, rr_name)

    def open_file_read(self, path: Path) -> ContextManager[BinaryIO]:
        return self._iso_rr.open_file_from_iso(make_rr_path(path))

    def open_file_write(self, path: Path, file_mode: int = None) -> ContextManager[BinaryIO]:
        temp_file = cast(BinaryIO, tempfile.NamedTemporaryFile(dir=str(self._temp_path), delete=False))
        rr_path, iso_path, rr_name = self._cdlib_paths(path, is_dir=False)
        return IsoFileWriter(
            temp_file=temp_file,
            add_file=lambda: self._iso.add_file(
                filename=temp_file.name,
                iso_path=iso_path,
                rr_name=rr_name,
                joliet_path=rr_path,
                file_mode=file_mode,
            ),
        )

    def open_file_replace(self, path: Path) -> ContextManager[BinaryIO]:
        record: DirectoryRecord = self._iso_rr.get_record(make_rr_path(path))
        rr_record: RockRidge = record.rock_ridge
        file_mode = rr_record.get_file_mode()
        self.remove_file(path)
        return self.open_file_write(path, file_mode=file_mode)

    def write_file(self, source: IO[bytes], path: Path, length: int, file_mode: int = None) -> None:
        rr_path, iso_path, rr_name = self._cdlib_paths(path, is_dir=False)
        self._iso.add_fp(
            fp=source,
            length=length,
            iso_path=iso_path,
            rr_name=rr_name,
            joliet_path=rr_path,
            file_mode=file_mode,
        )

    def replace_file(self, source: IO[bytes], path: Path, length: int) -> None:
        record: DirectoryRecord = self._iso_rr.get_record(make_rr_path(path))
        rr_record: RockRidge = record.rock_ridge
        file_mode = rr_record.get_file_mode()
        self.remove_file(path)
        return self.write_file(source, path, length, file_mode=file_mode)

    def remove_file(self, path: Path) -> None:
        rr_path, iso_path, rr_name = self._cdlib_paths(path, is_dir=False)
        self._iso.rm_file(iso_path=iso_path, rr_name=rr_name, joliet_path=rr_path)

    def copy_file(self, source_path: Path, path: Path) -> None:
        source_stat = source_path.stat().st_mode
        file_mode = 0o0100555 if source_stat & stat.S_IXUSR else None
        rr_path, iso_path, rr_name = self._cdlib_paths(path, is_dir=False)
        self._iso.add_file(
            str(source_path), iso_path=iso_path, rr_name=rr_name, joliet_path=rr_path, file_mode=file_mode
        )

    def create_directory(self, path: Path, file_mode: int = None) -> None:
        rr_path, iso_path, rr_name = self._cdlib_paths(path, is_dir=True)
        self._iso.add_directory(iso_path=iso_path, rr_name=rr_name, joliet_path=rr_path, file_mode=file_mode)

    def write_iso(self, path: Path, progress: ProgressReporter = None) -> None:
        task_id = progress.add_task("Write ISO") if progress else None
        logging.info(f"Start writing ISO to '{path}'.")
        progress_cb = functools.partial(write_progress_callback, progress) if progress else None
        self._iso.write(
            str(path), blocksize=SIZE_256KiB, progress_cb=progress_cb, progress_opaque=task_id,
        )
        logging.info("Finished writing ISO.")


def read_text(iso_file: IsoFile, path: Path) -> str:
    with iso_file.open_file_read(path) as file:
        return file.read().decode()


def write_text(iso_file: IsoFile, path: Path, content: str, file_mode: int = None) -> None:
    with iso_file.open_file_write(path, file_mode=file_mode) as file:
        file.write(content.encode())


def replace_text(iso_file: IsoFile, path: Path, content: str) -> None:
    with iso_file.open_file_replace(path) as file:
        file.write(content.encode())


def copy_directory(iso_file: IsoFile, source_path: Path, path: Path) -> None:
    iso_file.create_directory(path)
    for ipath in source_path.rglob("*"):
        dest_path = path / ipath.relative_to(source_path)
        if ipath.is_dir():
            iso_file.create_directory(dest_path)
        elif ipath.is_file():
            iso_file.copy_file(ipath, dest_path)
        else:
            raise Exception("Unhandle path type encountered during copy.")


# Private


def make_rr_path(path: Path) -> str:
    return str(path)


class IsoFileWriter(ContextManager[BinaryIO]):
    def __init__(self, temp_file: BinaryIO, add_file: Callable):
        self._temp_file = temp_file
        self._add_file = add_file

    def __enter__(self) -> BinaryIO:
        return self._temp_file

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self._temp_file.close()
        self._add_file()
        return None
