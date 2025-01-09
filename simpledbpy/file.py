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
        """Get an integer from the byte buffer

        Args:
            offset (int): The offset in the byte buffer

        Raises:
            ValueError: If the offset is out of bounds

        Returns:
            int: The integer at the specified offset
        """
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        return int(struct.unpack(self.FORMAT, self._byte_buffer[offset : offset + 4])[0])

    def set_int(self, offset: int, value: int) -> None:
        """Set an integer in the byte buffer

        Args:
            offset (int): The offset in the byte buffer
            value (int): The integer to set

        Raises:
            ValueError: If the offset is out of bounds
        """
        if len(self._byte_buffer) < offset + 4:
            raise ValueError("Byte buffer too small to set the given integer")
        self._byte_buffer[offset : offset + 4] = struct.pack(self.FORMAT, value)

    def get_bytes(self, offset: int) -> bytes:
        """Get a sequence of bytes from the byte buffer

        Args:
            offset (int): The offset in the byte buffer

        Raises:
            ValueError: If the offset is out of bounds

        Returns:
            bytes: The bytes at the specified offset
        """
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        length = self.get_int(offset)
        start = offset + struct.calcsize(self.FORMAT)
        byte_array = self._byte_buffer[start : start + length]
        return bytes(byte_array)

    def set_bytes(self, offset: int, b: bytes) -> None:
        """Set a sequence of bytes in the byte buffer

        Args:
            offset (int): The offset in the byte buffer
            b (bytes): The bytes to set

        Raises:
            ValueError: If the offset is out of bounds
        """
        total_length = struct.calcsize(self.FORMAT) + len(b)
        if len(self._byte_buffer) < offset + total_length:
            raise ValueError("Byte buffer too small to set the given bytes")
        self.set_int(offset, len(b))
        start = offset + struct.calcsize(self.FORMAT)
        self._byte_buffer[start : start + len(b)] = b

    def get_string(self, offset: int) -> str:
        """Get a string from the byte buffer

        Args:
            offset (int): The offset in the byte buffer

        Raises:
            ValueError: If the offset is out of bounds

        Returns:
            str: The string at the specified offset
        """
        if len(self._byte_buffer) <= offset:
            raise ValueError("Offset is out of bounds")
        b = self.get_bytes(offset)
        return b.decode(Page.CHARSET)

    def set_string(self, offset: int, s: str) -> None:
        """Set a string in the byte buffer

        Args:
            offset (int): The offset in the byte buffer
            s (str): The string to set
        """
        b = s.encode(Page.CHARSET)
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen: int) -> int:
        """Calculate the maximum length of a byte buffer

        Args:
            strlen (int): The length of the string

        Returns:
            int: The maximum length of the byte buffer
        """
        bytes_per_char = 1
        return 4 + strlen * bytes_per_char

    def contents(self) -> bytearray:
        """Get the byte buffer

        Returns:
            bytearray: The byte buffer
        """
        return self._byte_buffer


class FileManager:
    _db_directory: Path
    _block_size: int
    _is_new: bool
    _open_files: dict[str, BufferedRandom]
    _lock: Lock

    def __init__(self, db_directory: Path, block_size: int) -> None:
        """Constructor

        Args:
            db_directory (Path): The directory where the database files are stored
            block_size (int): The size of a block in bytes
        """
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
        """Read a block into a page

        Args:
            block_id (BlockId): The block to read
            page (Page): The page to read into

        Raises:
            IOError: If the block cannot be read
        """
        with self._lock:
            try:
                f = self._get_file(block_id.filename)
                f.seek(block_id.block_number * self._block_size)
                f.readinto(page.contents())
            except IOError as e:
                raise IOError(f"Cannot read block {block_id}") from e

    def write(self, block_id: BlockId, page: Page) -> None:
        """Write a page to a block

        Args:
            block_id (BlockId): The block to write to
            page (Page): The page to write

        Raises:
            IOError: If the block cannot be written
        """
        with self._lock:
            try:
                f = self._get_file(block_id.filename)
                f.seek(block_id.block_number * self._block_size)
                f.write(page.contents())
                f.flush()
            except IOError as e:
                raise IOError(f"Cannot write block {block_id}") from e

    def append(self, filename: str) -> BlockId:
        """Append a block to a file

        Args:
            filename (str): The name of the file

        Raises:
            IOError: If the block cannot be appended

        Returns:
            BlockId: The block that was appended
        """
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
        """Get the number of blocks in a file

        Args:
            filename (str): The name of the file

        Raises:
            IOError: If the file cannot be accessed

        Returns:
            int: The number of blocks in the file
        """
        try:
            f = self._get_file(filename)
            return int(f.seek(0, os.SEEK_END) / self._block_size)
        except IOError as e:
            raise IOError(f"Cannot access {filename}") from e

    def _get_file(self, filename: str) -> BufferedRandom:
        """Get a file object

        Args:
            filename (str): The name of the file

        Returns:
            BufferedRandom: The file object
        """
        f = self._open_files.get(filename)
        if f is None:
            db_table = self._db_directory / filename
            if not db_table.exists():
                db_table.touch()
            f = db_table.open("r+b")
            self._open_files[filename] = f
        return f

    def __del__(self) -> None:
        for f in self._open_files.values():
            f.close()
        self._open_files.clear()
