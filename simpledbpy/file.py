import os
import struct
from dataclasses import dataclass
from io import BufferedRandom
from pathlib import Path
from threading import Lock


@dataclass(frozen=True)
class BlockId:
    filename: str
    block_number: int

    def __str__(self) -> str:
        return f"[file {self.filename}, block {self.block_number}]"


class Page:
    CHARSET = "ascii"
    FORMAT = ">i"

    def __init__(self, args: int | bytes) -> None:
        """Constructor

        Args:
            args (int | bytes):
                If int, creates a byte buffer of the specified size.
                If bytes, creates a byte buffer with the specified contents.

        Raises:
            ValueError:
                If the argument is not an int or bytes.
        """
        if isinstance(args, int):
            self._byte_buffer = bytearray(args)
        elif isinstance(args, bytes):
            self._byte_buffer = bytearray(args)
        else:
            raise ValueError("Invalid argument type")

    def get_int(self, offset: int) -> int:
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        return int(
            struct.unpack(self.FORMAT, self._byte_buffer[offset : offset + 4])[0]
        )

    def set_int(self, offset: int, value: int) -> None:
        if len(self._byte_buffer) < offset + 4:
            raise ValueError("Byte buffer too small to set the given integer")
        self._byte_buffer[offset : offset + 4] = struct.pack(self.FORMAT, value)

    def get_bytes(self, offset: int) -> bytes:
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        length = self.get_int(offset)
        start = offset + struct.calcsize(self.FORMAT)
        byte_array = self._byte_buffer[start : start + length]
        return bytes(byte_array)

    def set_bytes(self, offset: int, b: bytes) -> None:
        total_length = struct.calcsize(self.FORMAT) + len(b)
        if len(self._byte_buffer) < offset + total_length:
            raise ValueError("Byte buffer too small to set the given bytes")
        self.set_int(offset, len(b))
        start = offset + struct.calcsize(self.FORMAT)
        self._byte_buffer[start : start + len(b)] = b

    def get_string(self, offset: int) -> str:
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        b = self.get_bytes(offset)
        return b.decode(Page.CHARSET)

    def set_string(self, offset: int, s: str) -> None:
        b = s.encode(Page.CHARSET)
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen: int) -> int:
        bytes_per_char = 1
        return 4 + strlen * bytes_per_char

    def contents(self) -> bytearray:
        return self._byte_buffer


class FileManager:
    _db_directory: Path
    _block_size: int
    _is_new: bool
    _open_files: dict[str, BufferedRandom]
    _lock: Lock

    def __init__(self, db_directory: Path, block_size: int) -> None:
        self._db_directory = db_directory
        self._block_size = block_size
        self._is_new = not db_directory.exists()

        # create the directory if the database is new
        if self._is_new:
            db_directory.mkdir(parents=True)

        # remove any leftover temporary tables
        for file in self._db_directory.iterdir():
            if file.name.startswith("temp"):
                file.unlink()

        self._open_files = {}
        self._lock = Lock()

    def read(self, block_id: BlockId, page: Page) -> None:
        with self._lock:
            try:
                f = self._get_file(block_id.filename)
                f.seek(block_id.block_number * self._block_size)
                f.readinto(page.contents())
            except IOError as e:
                raise IOError(f"Cannot read block {block_id}") from e

    def write(self, block_id: BlockId, page: Page) -> None:
        with self._lock:
            try:
                f = self._get_file(block_id.filename)
                f.seek(block_id.block_number * self._block_size)
                f.write(page.contents())
                f.flush()
            except IOError as e:
                raise IOError(f"Cannot write block {block_id}") from e

    def append(self, filename: str) -> BlockId:
        with self._lock:
            new_block_number = self.length(filename)
            block = BlockId(filename, new_block_number)
            b = bytearray(self._block_size)
            try:
                f = self._get_file(filename)
                f.seek(new_block_number * self._block_size)
                f.write(b)
                f.flush()
            except IOError as e:
                raise IOError(f"Cannot append block {block}") from e
            return block

    def length(self, filename: str) -> int:
        try:
            f = self._get_file(filename)
            return int(f.seek(0, os.SEEK_END) / self._block_size)
        except IOError as e:
            raise IOError(f"Cannot access {filename}") from e

    def _get_file(self, filename: str) -> BufferedRandom:
        f = self._open_files.get(filename)
        if f is None:
            db_table = self._db_directory / filename
            if not db_table.exists():
                db_table.touch()
            f = db_table.open("r+b")
            self._open_files[filename] = f
        return f
