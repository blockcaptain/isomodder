import io
import itertools
import re
import stat
import tarfile
from pathlib import Path
from typing import BinaryIO, List, Optional, Union, cast

from ._hashing import HashFileRecord, create_hash_file_record, parse_hash_file, write_hash_file
from ._iso import IsoFile, copy_directory, read_text, replace_text, write_text


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
        self._new_hashes: List[HashFileRecord] = []
        self._grub_hash: Optional[HashFileRecord] = None
        self._supports_mbr = supports_mbr
        self._supports_efi = supports_efi

    def add_from_directory(self, source_path: Path, iso_path: Path) -> None:
        copy_directory(self._source_iso, source_path, iso_path)
        new_records = (
            create_hash_file_record(ipath.open("rb"), iso_path / ipath.relative_to(source_path))
            for ipath in source_path.rglob("*")
            if ipath.is_file()
        )
        self._new_hashes.extend(new_records)

    def add_from_tar(self, source: Union[Path, BinaryIO], iso_path: Path) -> None:
        source_fp = source.open("rb") if isinstance(source, Path) else source
        tar_file = tarfile.open(fileobj=source_fp, mode="r")
        self._source_iso.create_directory(iso_path)
        infos = (info for info in iter(tar_file.next, None) if info.name != ".")
        for info in infos:
            path = iso_path / info.name
            if info.isdir():
                self._source_iso.create_directory(path)
            else:
                with cast(BinaryIO, tar_file.extractfile(info)) as fp:
                    self._new_hashes.append(create_hash_file_record(fp, path))

                file_mode = 0o0100555 if info.mode & stat.S_IXUSR else None
                fp = cast(BinaryIO, tar_file.extractfile(info))
                self._source_iso.write_file(fp, path, length=info.size, file_mode=file_mode)

    def build(self) -> None:
        self._add_autoinstall()
        self._update_grub()
        self._update_hash_file()

    def _update_hash_file(self) -> None:
        md5_path = Path("/md5sum.txt")
        with self._source_iso.open_file_read(md5_path) as file:
            records = parse_hash_file(io.TextIOWrapper(file))
            grub_path_str = f".{self.grub_path}"
            assert self._grub_hash is not None
            new_records = list(
                itertools.chain((r for r in records if r.path != grub_path_str), [self._grub_hash])
            )
        with self._source_iso.open_file_replace(md5_path) as file:
            write_hash_file(io.TextIOWrapper(file), new_records)

    def _update_grub(self) -> None:
        grub_text = read_text(self._source_iso, self.grub_path)
        isolinux_text = read_text(self._source_iso, self.isolinux_path)

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
        self._grub_hash = create_hash_file_record(io.BytesIO(grub_text.encode()), self.grub_path)

        replace_text(self._source_iso, self.grub_path, grub_text)
        replace_text(
            self._source_iso,
            self.isolinux_path,
            modify_text(isolinux_text, None if self._supports_mbr else "MBR", False),
        )

    def _add_autoinstall(self) -> None:
        ds_path = Path("/nocloud")
        self._source_iso.create_directory(ds_path)
        write_text(self._source_iso, ds_path / "user-data", self._autoinstall_yaml)
        write_text(self._source_iso, ds_path / "meta-data", "\n")
