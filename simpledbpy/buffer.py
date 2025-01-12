import threading
import time
from typing import Sequence

from simpledbpy.file import BlockId, FileManager, Page
from simpledbpy.log import LogManager


class BufferAbortError(RuntimeError):
    pass


class Buffer:
    """An individual buffer. A databuffer wraps a page and stores information about its status,
    such as the associated disk block, the number of times the buffer has been pinned,
    whether its contents have been modified, and if so, the id and lsn of the modifying transaction.
    """

    _file_manager: FileManager
    _log_manager: LogManager
    _contents: Page
    _block_id: BlockId | None
    _pins: int
    _txnum: int
    _lsn: int

    def __init__(self, file_manager: FileManager, log_manager: LogManager):
        self._file_manager = file_manager
        self._log_manager = log_manager
        self._contents = Page(file_manager.block_size)
        self._block_id = None
        self._pins = 0
        self._txnum = -1
        self._lsn = -1

    @property
    def contents(self) -> Page:
        """Get the contents of the buffer

        Returns:
            Page: The contents of the buffer
        """
        return self._contents

    @property
    def block_id(self) -> BlockId | None:
        """Get the block id of the buffer

        Returns:
            BlockId: The block id of the buffer
        """
        return self._block_id

    def set_modified(self, txnum: int, lsn: int) -> None:
        """Set the buffer as modified

        Args:
            txnum (int): the modifying transaction
            lsn (int): the corresponding log sequence number
        """
        self._txnum = txnum
        if lsn >= 0:
            self._lsn = lsn

    def is_pinned(self) -> bool:
        """Check if the buffer is pinned

        Returns:
            bool: True if the buffer is pinned, False otherwise
        """
        return self._pins > 0

    @property
    def modifying_txnum(self) -> int:
        """Get the modifying transaction number

        Returns:
            int: The modifying transaction number
        """
        return self._txnum

    def assign_to_block(self, block_id: BlockId) -> None:
        """Assign the buffer to a block

        Args:
            block_id (BlockId): The block id
        """
        self.flush()  # flush the buffer before reassigning
        self._block_id = block_id
        self._file_manager.read(self._block_id, self._contents)
        self._pins = 0

    def flush(self) -> None:
        """Flush the buffer to disk"""
        if self._txnum >= 0 and self._block_id is not None:
            self._log_manager.flush(self._lsn)
            self._file_manager.write(self._block_id, self._contents)
            self._txnum = -1

    def pin(self) -> None:
        """Pin the buffer"""
        self._pins += 1

    def unpin(self) -> None:
        """Unpin the buffer"""
        self._pins -= 1


class BufferManager:
    """Manages the pinning and unpinning of buffers to blocks."""

    MAX_TIME = 10

    _buffer_pool: Sequence[Buffer]
    _num_available: int
    _cv: threading.Condition

    def __init__(self, file_manage: FileManager, log_manager: LogManager, num_buffers: int):
        """Create a buffer manager having the specified number of buffers.

        Args:
            file_manage (FileManager): the file manager
            log_manager (LogManager): the log manager
            num_buffers (int): the number of buffer slots
        """
        self._buffer_pool = []
        self._num_available = num_buffers
        for _ in range(num_buffers):
            self._buffer_pool.append(Buffer(file_manage, log_manager))
        self._cv = threading.Condition()

    @property
    def available(self) -> int:
        """Get the number of available buffers

        Returns:
            int: The number of available buffers
        """
        with self._cv:
            return self._num_available

    def flush_all(self, txnum: int) -> None:
        """Flush all buffers associated with a transaction

        Args:
            txnum (int): the transaction number to flush
        """
        with self._cv:
            for buffer in self._buffer_pool:
                if buffer.modifying_txnum == txnum:
                    buffer.flush()

    def unpin(self, buffer: Buffer) -> None:
        """Unpin the specified buffer

        Args:
            buffer (Buffer): the buffer to unpin
        """
        with self._cv:
            buffer.unpin()
            if not buffer.is_pinned():
                self._num_available += 1
                self._cv.notify_all()

    def pin(self, block_id: BlockId) -> Buffer:
        """Pin a buffer to the specified block

        Args:
            block_id (BlockId): the block id

        Raises:
            BufferAbortError: if the buffer cannot be pinned

        Returns:
            Buffer: the buffer pinned to the block
        """
        with self._cv:
            timestamp = time.time()
            buffer = self._try_to_pin(block_id)
            while buffer is None and self._waiting_too_long(timestamp):
                self._cv.wait(BufferManager.MAX_TIME)
                buffer = self._try_to_pin(block_id)
            if buffer is None:
                raise BufferAbortError()
            return buffer

    def _waiting_too_long(self, start_time: float) -> bool:
        """Check if the waiting time is longer than the maximum time

        Args:
            start_time (float): the start time of the try to pin

        Returns:
            bool: True if the waiting time is longer than the maximum time, False otherwise
        """
        return time.time() - start_time > BufferManager.MAX_TIME

    def _try_to_pin(self, block_id: BlockId) -> Buffer | None:
        """Try to pin a buffer to the specified block

        Args:
            block_id (BlockId): the block id to pin

        Returns:
            Buffer | None: the buffer pinned to the block, or None if the buffer cannot be pinned
        """
        buffer = self._find_existing_buffer(block_id)
        if buffer is None:
            buffer = self._choose_unpinned_buffer()
            if buffer is None:
                return None
            buffer.assign_to_block(block_id)
        if not buffer.is_pinned():
            self._num_available -= 1
        buffer.pin()
        return buffer

    def _find_existing_buffer(self, block_id: BlockId) -> Buffer | None:
        """Find an existing buffer pinned to the specified block

        Args:
            block_id (BlockId): the block id to find

        Returns:
            Buffer | None: the buffer pinned to the block, or None if no buffer is pinned to the block
        """
        for buffer in self._buffer_pool:
            if buffer.block_id is not None and buffer.block_id == block_id:
                return buffer
        return None

    def _choose_unpinned_buffer(self) -> Buffer | None:
        """Choose an unpinned buffer

        Returns:
            Buffer | None: the unpinned buffer, or None if no buffer is unpinned
        """
        for buffer in self._buffer_pool:
            if not buffer.is_pinned():
                return buffer
        return None
