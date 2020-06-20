import contextlib
import functools
import io
import logging
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Any, Callable

import pycdlib
from pycdlib.dr import DirectoryRecord
from pycdlib.facade import PyCdlibRockRidge
from pycdlib.rockridge import RockRidge

from ._progress import ProgressReporter, SIZE_256KiB


def write_progress_callback(progress, completed, total, task_id):
    progress.update(task_id, completed=completed, total=total)


def make_rr_path(path: Path) -> str:
    return str(path)


class IsoFile(contextlib.AbstractContextManager):
    def __init__(self, path: Path, working_dir: Path = None):
        self._iso = pycdlib.PyCdlib()
        self._iso.open(str(path))
        self._iso_rr: PyCdlibRockRidge = self._iso.get_rock_ridge_facade()
        self._temp_path_obj = None
        self._working_dir = working_dir

    def __exit__(self, exc_type, exc_value, traceback):
        self._iso.close()
        if self._temp_path_obj is not None:
            self._temp_path_obj.cleanup()
        return None

    @property
    def _temp_path(self) -> Path:
        if self._temp_path_obj is None:
            self._temp_path_obj = tempfile.TemporaryDirectory(dir=self._working_dir)
        return Path(self._temp_path_obj.name)

    def read_text(self, path: Path) -> str:
        with self.open_file_read(path) as file:
            return file.readall().decode()

    def write_text(self, path: Path, content: str, file_mode: int = None) -> None:
        with self.open_file_write(make_rr_path(path), file_mode=file_mode) as file:
            file.write(content.encode())

    def write_fp(self, path: Path, fp: io.IOBase, length: int, file_mode: int = None) -> None:
        rr_path = make_rr_path(path)
        iso_path, rr_name = self._iso_rr._rr_path_to_iso_path_and_rr_name(rr_path, is_dir=False)
        self._iso.add_fp(
            fp=fp,
            length=length,
            iso_path=iso_path,
            rr_name=rr_name,
            joliet_path=rr_path,
            file_mode=file_mode,
        ),

    def replace_text(self, path: Path, content: str) -> str:
        with self.replace_file_write(make_rr_path(path)) as file:
            file.write(content.encode())

    def open_file_read(self, path: Path) -> io.RawIOBase:
        return self._iso_rr.open_file_from_iso(make_rr_path(path))

    def open_file_write(self, path: Path, file_mode: int = None) -> io.RawIOBase:
        temp_file = tempfile.NamedTemporaryFile(dir=str(self._temp_path), delete=False)
        rr_path = make_rr_path(path)
        iso_path, rr_name = self._iso_rr._rr_path_to_iso_path_and_rr_name(rr_path, is_dir=False)
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

    def replace_file_write(self, path: Path) -> io.RawIOBase:
        rr_path = make_rr_path(path)
        record: DirectoryRecord = self._iso_rr.get_record(rr_path)
        rr_record: RockRidge = record.rock_ridge
        file_mode = rr_record.get_file_mode()
        self._iso_rr.rm_file(rr_path)
        return self.open_file_write(rr_path, file_mode=file_mode)

    def copy_file(self, source_path: Path, path: Path) -> None:
        source_stat = source_path.stat().st_mode
        file_mode = 0o0100555 if source_stat & stat.S_IXUSR else None
        self._iso_rr.add_file(str(source_path), make_rr_path(path), file_mode=file_mode)

    def create_directory(self, path: Path, file_mode: int = None) -> None:
        rr_path = make_rr_path(path)
        iso_path, rr_name = self._iso_rr._rr_path_to_iso_path_and_rr_name(rr_path, is_dir=True)
        self._iso.add_directory(iso_path=iso_path, rr_name=rr_name, joliet_path=rr_path, file_mode=file_mode)

    def copy_directory(self, source_path: Path, path: Path) -> None:
        self.create_directory(path)
        for ipath in source_path.rglob("*"):
            dest_path = path / ipath.relative_to(source_path)
            if ipath.is_dir():
                self.create_directory(dest_path)
            elif ipath.is_file():
                self.copy_file(ipath, dest_path)
            else:
                raise Exception("Unhandle path type encountered during copy.")

    def write_iso(self, path: Path, progress: ProgressReporter = None) -> None:
        task_id = progress.add_task("Write ISO")
        logging.info(f"Start writing ISO to '{path}'.")
        progress_cb = functools.partial(write_progress_callback, progress) if progress else None
        self._iso.write(
            str(path), blocksize=SIZE_256KiB, progress_cb=progress_cb, progress_opaque=task_id,
        )
        logging.info(f"Finished writing ISO.")


class IsoFileWriter(contextlib.AbstractContextManager):
    def __init__(self, temp_file: tempfile.NamedTemporaryFile, add_file: Callable):
        self._temp_file = temp_file
        self._add_file = add_file

    def __enter__(self):
        return self._temp_file

    def __exit__(self, exc_type, exc_value, traceback):
        self._temp_file.close()
        self._add_file()
        return None
