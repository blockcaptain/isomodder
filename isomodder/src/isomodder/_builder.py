import re
from pathlib import Path
from typing import Optional
from . import _hashing as hashing
from ._iso import IsoFile
import io
import itertools


class AutoInstallBuilder(object):
    grub_path = Path("/boot/grub/grub.cfg")
    isolinux_path = Path("/isolinux/txt.cfg")

    def __init__(
        self,
        source_iso: IsoFile,
        autoinstall_yaml: str,
        grub_entry_stamp: str = "AutoInstall",
        autoinstall_prompt: bool = True,
        supports_mbr: bool = True,
        supports_efi: bool = True,
    ):
        self._source_iso = source_iso
        self._autoinstall_yaml = autoinstall_yaml
        self._grub_stamp = grub_entry_stamp
        self._prompt = autoinstall_prompt
        self._additional_dirs = []
        self._supports_mbr = supports_mbr
        self._supports_efi = supports_efi
        self._grub_digest = None

    def add_directory(self, source_path: Path, iso_path: Path) -> None:
        self._source_iso.copy_directory(source_path, iso_path)
        self._additional_dirs.append((source_path, iso_path))

    def build(self) -> None:
        self._add_autoinstall()
        self._update_grub()
        self._update_hash_file()

    def _update_hash_file(self) -> None:
        md5_path = Path("/md5sum.txt")
        with self._source_iso.open_file_read(md5_path) as file:
            records = hashing.parse_hash_file(io.TextIOWrapper(file))
            grub_path_str = f".{self.grub_path}"
            new_records = list(
                itertools.chain((r for r in records if r.path != grub_path_str), [self._grub_digest])
            )
        with self._source_iso.replace_file_write(md5_path) as file:
            hashing.write_hash_file(io.TextIOWrapper(file), new_records)

    def _update_grub(self) -> None:
        grub_text = self._source_iso.read_text(self.grub_path)
        isolinux_text = self._source_iso.read_text(self.isolinux_path)

        def modify_text(text: str, unsupported_mode: Optional[str], escape_semicolon: bool) -> str:
            stamp = (
                f"*AutoInstall with {unsupported_mode} boot not supported by this ISO*"
                if unsupported_mode
                else f"({self._grub_stamp})"
            )
            text = re.sub("Install Ubuntu Server", f"\\g<0> {stamp}", text)
            autoinstall = "" if self._prompt else "autoinstall"
            escape = "\\" if escape_semicolon else ""
            text = re.sub(
                r"---\s*$",
                f"{autoinstall} ds=nocloud{escape};s=/cdrom/nocloud/ ---",
                text,
                flags=re.MULTILINE,
            )
            return text

        grub_text = modify_text(grub_text, None if self._supports_efi else "EFI", True)
        self._grub_digest = hashing.compute_digest(io.BytesIO(grub_text.encode()), self.grub_path)

        self._source_iso.replace_text(self.grub_path, grub_text)
        self._source_iso.replace_text(
            self.isolinux_path, modify_text(isolinux_text, None if self._supports_mbr else "MBR", False)
        )

    def _add_autoinstall(self) -> None:
        ds_path = Path("/nocloud")
        self._source_iso.create_directory(ds_path)
        self._source_iso.write_text(ds_path / "user-data", self._autoinstall_yaml)
        self._source_iso.write_text(ds_path / "meta-data", "\n")
