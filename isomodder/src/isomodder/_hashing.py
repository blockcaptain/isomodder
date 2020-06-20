import io
from typing import Iterable, NamedTuple


class HashFileRecord(NamedTuple):
    path: str
    digest: str


def parse_hash_file(fp: io.TextIOBase) -> Iterable[HashFileRecord]:
    for line in fp:
        parts = line.strip().split(maxsplit=1)
        yield HashFileRecord(parts[1].lstrip("*"), parts[0])


def write_hash_file(fp: io.TextIOBase, records: Iterable[HashFileRecord]) -> None:
    fp.writelines(f"{record.digest}\t\t{record.path}" for record in records)
