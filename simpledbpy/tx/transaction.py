from simpledbpy.buffer import Buffer, BufferManager
from simpledbpy.file import BlockId, FileManager
from simpledbpy.log import LogManager
from simpledbpy.tx.concurrency import ConcurrencyManager
from simpledbpy.tx.recovery import RecoveryManager


class BufferList:
    """Manage the transaction's currently-pinned buffers."""

    _buffers: dict[BlockId, Buffer]
    _pins: list[BlockId]
    _buffer_manager: BufferManager

    def __init__(self, buffer_manager: BufferManager) -> None:
        self._buffers = {}
        self._pins = []
        self._buffer_manager = buffer_manager

    def get_buffer(self, block_id: BlockId) -> Buffer:
        """Return the buffer pinned to the specified block.
        The method returns null if the transaction has not pinned the block.

        Args:
            block_id (BlockId): The block to be pinned.

        Returns:
            Buffer: The buffer pinned to the specified block.

        """
        buffer = self._buffers.get(block_id)
        if buffer is None:
            raise RuntimeError()
        return buffer

    def pin(self, block_id: BlockId) -> None:
        """Pin the block and keep track of the buffer internally.

        Args:
            block_id (BlockId): a reference to the disk block
        """
        buffer = self._buffer_manager.pin(block_id)
        self._buffers[block_id] = buffer
        self._pins.append(block_id)

    def unpin(self, block_id: BlockId) -> None:
        """Unpin the specified block.

        Args:
            block_id (BlockId): a reference to the disk block
        """
        buffer = self._buffers.get(block_id)
        if buffer is not None:
            buffer.unpin()
            self._pins.remove(block_id)
            if block_id not in self._pins:
                del self._buffers[block_id]

    def unpin_all(self) -> None:
        """Unpin any buffers still pinned by this transaction."""
        for block_id in self._pins:
            buffer = self._buffers.get(block_id)
            if buffer is not None:
                self._buffer_manager.unpin(buffer)
        self._buffers.clear()
        self._pins.clear()


class Transaction:
    """Provide transaction management for clients, ensuring that all transactions are serializable, recoverable,
    and in general satisfy the ACID properties.
    """

    END_OF_FILE = -1

    _next_txnum: int = 0
    _recovery_manager: RecoveryManager
    _concurrency_manager: ConcurrencyManager
    _buffer_manager: BufferManager
    _file_manager: FileManager
    _txnum: int
    _my_buffers: BufferList

    def __init__(self, file_manager: FileManager, log_manager: LogManager, buffer_manager: BufferManager) -> None:
        """Create a new transaction and its associated recovery and concurrency managers.
        This constructor depends on the file, log, and buffer managers.
        Those objects are created during system initialization.

        Args:
            file_manager (FileManager): file manager
            log_manager (LogManager): log manager
            buffer_manager (BufferManager): buffer manager
        """
        self._file_manager = file_manager
        self._buffer_manager = buffer_manager
        self._txnum = self._next_tx_number()
        self._recovery_manager = RecoveryManager(self, self._txnum, log_manager, self._buffer_manager)
        self._concurrency_manager = ConcurrencyManager()
        self._my_buffers = BufferList(self._buffer_manager)

    def commit(self) -> None:
        """Commit the current transaction.
        Flush all modified buffers (and their log records),
        write and flush a commit record to the log,
        release all locks, and unpin any pinned buffers.
        """
        self._recovery_manager.commit()
        print(f"Transaction {self._txnum} committed")
        self._concurrency_manager.release()
        self._my_buffers.unpin_all()

    def rollback(self) -> None:
        """Rollback the current transaction.
        Undo any modified values, flush those buffers,
        write and flush a rollback record to the log,
        release all locks, and unpin any pinned buffers.
        """
        self._recovery_manager.rollback()
        print(f"Transaction {self._txnum} rolled back")
        self._concurrency_manager.release()
        self._my_buffers.unpin_all()

    def recover(self) -> None:
        """Flush all modified buffers.
        Then go through the log, rolling back all uncommitted transactions.
        Finally, write a quiescent checkpoint record to the log.
        This method is called during system startup, before user transactions begin.
        """
        self._buffer_manager.flush_all(self._txnum)
        self._recovery_manager.recover()

    def pin(self, block_id: BlockId) -> None:
        """Pin the specified block. The transaction manages the buffer for the client.

        Args:
            block_id (BlockId): a reference to the disk block
        """
        self._my_buffers.pin(block_id)

    def unpin(self, block_id: BlockId) -> None:
        """Unpin the specified block. The transaction looks up the buffer pinned to this block, and unpins it.

        Args:
            block_id (BlockId): a reference to the disk block
        """
        self._my_buffers.unpin(block_id)

    def get_int(self, block_id: BlockId, offset: int) -> int:
        """Return the integer value stored at the specified offset of the specified block.
        The method first obtains an SLock on the block, then it calls the buffer to retrieve the value.

        Args:
            block_id (BlockId): a reference to the disk block
            offset (int): the byte offset within the block

        Returns:
            int: the integer stored at that offset
        """
        self._concurrency_manager.slock(block_id)
        buffer = self._my_buffers.get_buffer(block_id)
        return buffer.contents.get_int(offset)

    def get_string(self, block_id: BlockId, offset: int) -> str:
        """Return the string value stored at the specified offset of the specified block.
        The method first obtains an SLock on the block, then it calls the buffer to retrieve the value.

        Args:
            block_id (BlockId): a reference to the disk block
            offset (int): the byte offset within the block

        Returns:
            str: the string stored at that offset
        """
        self._concurrency_manager.slock(block_id)
        buffer = self._my_buffers.get_buffer(block_id)
        return buffer.contents.get_string(offset)

    def set_int(self, block_id: BlockId, offset: int, value: int, ok_to_log: bool) -> None:
        """Store an integer at the specified offset of the specified block.
        The method first obtains an XLock on the block. It then reads the current value at that offset,
        puts it into an update log record, and writes that record to the log.
        Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.

        Args:
            block_id (BlockId): a reference to the disk block
            offset (int): the byte offset within the block
            value (int): the value to be stored
            ok_to_log (bool): a flag indicating whether or not the update should be logged
        """
        self._concurrency_manager.xlock(block_id)
        buffer = self._my_buffers.get_buffer(block_id)
        lsn = -1
        if ok_to_log:
            lsn = self._recovery_manager.set_int(buffer, offset, value)
        page = buffer.contents
        page.set_int(offset, value)
        buffer.set_modified(self._txnum, lsn)

    def set_string(self, block_id: BlockId, offset: int, value: str, ok_to_log: bool) -> None:
        """Store a string at the specified offset of the specified block.
        The method first obtains an XLock on the block. It then reads the current value at that offset,
        puts it into an update log record, and writes that record to the log.
        Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.

        Args:
            block_id (BlockId): a reference to the disk block
            offset (int): the byte offset within the block
            value (str): the value to be stored
            ok_to_log (bool): a flag indicating whether or not the update should be logged
        """
        self._concurrency_manager.xlock(block_id)
        buffer = self._my_buffers.get_buffer(block_id)
        lsn = -1
        if ok_to_log:
            lsn = self._recovery_manager.set_string(buffer, offset, value)
        page = buffer.contents
        page.set_string(offset, value)
        buffer.set_modified(self._txnum, lsn)

    def size(self, filename: str) -> int:
        """Return the number of blocks in the specified file.
        This method first obtains an SLock on the "end of the file",
        before asking the file manager to return the file size.

        Args:
            filename (str):

        Returns:
            int: _description_
        """
        dummy_block = BlockId(filename, self.END_OF_FILE)
        self._concurrency_manager.slock(dummy_block)
        return self._file_manager.length(filename)

    def append(self, filename: str) -> BlockId:
        """Append a new block to the end of the specified file and returns a reference to it.
        This method first obtains an XLock on the "end of the file", before performing the append.

        Args:
            filename (str): the name of the file

        Returns:
            BlockId: a reference to the newly-created disk block
        """
        dummy_block = BlockId(filename, self.END_OF_FILE)
        self._concurrency_manager.xlock(dummy_block)
        return self._file_manager.append(filename)

    @property
    def block_size(self) -> int:
        return self._file_manager.block_size

    @property
    def available_buffers(self) -> int:
        return self._buffer_manager.available

    def _next_tx_number(self) -> int:
        self._next_txnum += 1
        return self._next_txnum
