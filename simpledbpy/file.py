import struct
from dataclasses import dataclass


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

    def contents(self) -> bytes:
        return bytes(self._byte_buffer)
