import hashlib
import io
from pathlib import Path
from typing import Iterable, NamedTuple


class HashFileRecord(NamedTuple):
    path: str
    digest: str


def parse_hash_file(fp: io.TextIOBase) -> Iterable[HashFileRecord]:
    for line in fp:
        parts = line.strip().split(maxsplit=1)
        yield HashFileRecord(parts[1].lstrip("*"), parts[0])


def write_hash_file(fp: io.TextIOBase, records: Iterable[HashFileRecord]) -> None:
    fp.writelines(f"{record.digest}  {record.path}\n" for record in records)


def compute_digest(fp: io.RawIOBase, path: Path) -> HashFileRecord:
    return HashFileRecord("." + str(path), hashlib.md5(fp.read()).hexdigest())
