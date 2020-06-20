import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Tuple
from urllib.request import urlopen

from ._failure import IsoModderFatalException
from ._hashing import parse_hash_file
from ._progress import ProgressReporter, chunk_stream


def download(
    task_id: Any, description: str, url: str, file_path: Path, progress_instance: ProgressReporter,
) -> None:
    response = urlopen(url)
    total_length = int(response.info()["Content-length"])
    if progress_instance:
        progress_instance.update(task_id, total=total_length)
    else:
        logging.info(f"{description} download is {total_length / 1024**2} MiBs.")

    with file_path.open("wb") as dest_file:
        if progress_instance:
            progress_instance.start_task(task_id)
        logging.info(f"Start downloading {description} from '{url}'.")
        for data in chunk_stream(response):
            dest_file.write(data)
            if progress_instance:
                progress_instance.update(task_id, advance=len(data))
        logging.info(f"Finished downloading {description}.")


def download_all(
    urls: Iterable[Tuple[str, str]], directory_path: Path, progress_instance: ProgressReporter,
) -> None:
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for url in urls:
            filename = url[1].split("/")[-1]
            path = directory_path / filename
            task_id = progress_instance.add_task(url[0], start=False) if progress_instance else None
            futures.append(
                pool.submit(download, task_id, url[0], url[1], directory_path / filename, progress_instance,)
            )
        for f in as_completed(futures):
            f.result()


def delete_if_exist(path: Path) -> None:
    if path.exists():
        path.unlink()


class BaseIsoFetcher(object):
    def __init__(self, working_dir: Path, base_url: str, iso_rel_url: str, checksum_rel_url: str):
        self._working_dir = working_dir
        self._base_url = base_url
        self._iso_name = iso_rel_url
        self._checksum_name = checksum_rel_url

    @property
    def _iso_path(self) -> Path:
        return self._working_dir / self._iso_name

    @property
    def _checksum_path(self) -> Path:
        return self._working_dir / self._checksum_name

    @property
    def _validation_path(self) -> Path:
        return self._iso_path.with_suffix(".validated")

    def _clear(self) -> None:
        for p in [self._checksum_path, self._validation_path, self._iso_path]:
            delete_if_exist(p)

    def fetch(self, progress: ProgressReporter = None) -> Path:
        logging.info("Checking for existing ISO.")
        if self._is_validated():
            logging.info(f"Found existing validated ISO.")
            return self._iso_path

        if not self._need_download():
            logging.info("Removing existing unvalidated ISO.")
            self._clear()

        logging.info("Start downloading ISO.")
        self._download(progress)

        logging.info("Start validating ISO.")
        if self._validate():
            logging.info(f"Validated ISO.")
        else:
            raise IsoModderFatalException("ISO validation failed. Re-run to attempt the download again.")

        return self._iso_path

    def _need_download(self) -> bool:
        return not self._iso_path.exists() or not self._checksum_path.exists()

    def _download(self, progress: ProgressReporter) -> None:
        download_all(
            [
                ("ISO", f"{self._base_url}/{self._iso_name}"),
                ("Checksums", f"{self._base_url}/{self._checksum_name}"),
            ],
            self._working_dir,
            progress,
        )

    def _is_validated(self) -> bool:
        return (
            self._validation_path.exists()
            and self._iso_path.exists()
            and self._validation_path.stat().st_mtime >= self._iso_path.stat().st_mtime
        )

    def _validate(self) -> bool:
        hasher = hashlib.md5()
        with self._iso_path.open("rb") as iso_file:
            for data in chunk_stream(iso_file):
                hasher.update(data)
        desired = hasher.hexdigest()
        with self._checksum_path.open("r") as checksum_file:
            try:
                actual = next(h.digest for h in parse_hash_file(checksum_file) if h.path == self._iso_name)
            except StopIteration:
                raise IsoModderFatalException(
                    f"Could not find the digest for {self.iso_name} in {self._validation_path}."
                )
        if desired == actual:
            self._validation_path.touch()
            return True
        else:
            logging.error(f"For {self.iso_name}, expected md5: {desired}, actual md5: {actual}.")
            return False


class UbuntuServerIsoFetcher(BaseIsoFetcher):
    def __init__(self, working_dir: Path, release: str):
        base_url = f"https://releases.ubuntu.com/{release}"
        iso_name = f"ubuntu-{release}-live-server-amd64.iso"
        super().__init__(
            working_dir=working_dir, base_url=base_url, iso_rel_url=iso_name, checksum_rel_url="MD5SUMS",
        )
