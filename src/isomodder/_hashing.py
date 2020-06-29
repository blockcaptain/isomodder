import hashlib
from pathlib import Path
from typing import BinaryIO, Iterable, NamedTuple, TextIO

from ._progress import chunk_stream


class HashFileRecord(NamedTuple):
    path: str
    digest: str


def parse_hash_file(fp: TextIO) -> Iterable[HashFileRecord]:
    for line in fp:
        parts = line.strip().split(maxsplit=1)
        yield HashFileRecord(parts[1].lstrip("*"), parts[0])


def write_hash_file(fp: TextIO, records: Iterable[HashFileRecord]) -> None:
    fp.writelines(f"{record.digest}  {record.path}\n" for record in records)


def create_hash_file_record(fp: BinaryIO, path: Path) -> HashFileRecord:
    return HashFileRecord(canonical_hash_file_path(path), compute_digest(fp))


def canonical_hash_file_path(path: Path) -> str:
    return "." + str(path)


def compute_digest(fp: BinaryIO) -> str:
    hasher = hashlib.md5()
    for data in chunk_stream(fp):
        hasher.update(data)
    return hasher.hexdigest()
