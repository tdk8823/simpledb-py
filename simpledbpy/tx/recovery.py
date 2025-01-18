from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from simpledbpy.buffer import Buffer, BufferManager
from simpledbpy.file import BlockId, Page
from simpledbpy.log import LogManager

if TYPE_CHECKING:
    from simpledbpy.tx.transaction import Transaction


class LogType(Enum):
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5


class LogRecord(ABC):
    """The interface implemented by each type of log record."""

    @abstractmethod
    def op(self) -> LogType:
        """Returns the log record's type.

        Returns:
            LogType: the log record's type
        """
        raise NotImplementedError

    @abstractmethod
    def tx_number(self) -> int:
        """Returns the transaction id stored in the log record.

        Returns:
            int: the log record's transaction id
        """
        raise NotImplementedError

    @abstractmethod
    def undo(self, tx: Transaction) -> None:
        """Undoes the operation encoded by this log record.
        The only log record types for which this method does anything interesting are SETINT and SETSTRING.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        raise NotImplementedError

    @staticmethod
    def create_log_record(b: bytes) -> "LogRecord" | None:
        """Creates a log record by reading its values from the specified byte buffer."""
        page = Page(b)
        record_type = page.get_int(0)
        if record_type == LogType.CHECKPOINT.value:
            return CheckpointRecord()
        elif record_type == LogType.START.value:
            return StartRecord(page)
        elif record_type == LogType.COMMIT.value:
            return CommitRecord(page)
        elif record_type == LogType.ROLLBACK.value:
            return RollbackRecord(page)
        elif record_type == LogType.SETINT.value:
            return SetIntRecord(page)
        elif record_type == LogType.SETSTRING.value:
            return SetStringRecord(page)
        else:
            return None


class CheckpointRecord(LogRecord):
    """The CHECKPOINT log record."""

    def op(self) -> LogType:
        return LogType.CHECKPOINT

    def tx_number(self) -> int:
        """Checkpoint records have no associated transaction, and so the method returns a "dummy", negative txid.

        Returns:
            int: dummy value
        """
        return -1

    def undo(self, tx: Transaction) -> None:
        """Does nothing, because a checkpoint record contains no undo information."""
        pass

    def __str__(self) -> str:
        return "<CHECKPOINT>"

    @staticmethod
    def write_to_log(log_manager: LogManager) -> int:
        """A static method to write a checkpoint record to the log.
        This log record contains the CHECKPOINT operator, and nothing else.

        Args:
            log_manager (LogManager): the log manager

        Returns:
            int: the LSN of the last log value
        """
        integer_record_size = 4
        page = Page(integer_record_size)
        page.set_int(0, LogType.CHECKPOINT.value)
        return log_manager.append(page.contents())


class StartRecord(LogRecord):
    """Create a log record by reading one other value from the log."""

    _txnum: int

    def __init__(self, page: Page) -> None:
        """Create a log record by reading one other value from the log.

        Args:
            page (Page): the page containing the log values
        """
        txnum_position = 4  # Integer.BYTES
        self._txnum = page.get_int(txnum_position)

    def op(self) -> LogType:
        return LogType.START

    def tx_number(self) -> int:
        return self._txnum

    def undo(self, tx: Transaction) -> None:
        """Does nothing, because a start record contains no undo information.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        pass

    def __str__(self) -> str:
        return f"<START {self._txnum}>"

    @staticmethod
    def write_to_log(log_manager: LogManager, txnum: int) -> int:
        """A static method to write a start record to the log.
        This log record contains the START operator, followed by the transaction id.

        Args:
            log_manager (LogManager): the log manager
            txnum (int): the transaction id

        Returns:
            int: the LSN of the last log value
        """
        integer_record_size = 4
        page = Page(2 * integer_record_size)
        page.set_int(0, LogType.START.value)
        page.set_int(integer_record_size, txnum)
        return log_manager.append(page.contents())


class CommitRecord(LogRecord):
    """The COMMIT log record."""

    _txnum: int

    def __init__(self, page: Page) -> None:
        """Create a new commit log record.

        Args:
            page (Page): the page containing the log values
        """
        txnum_position = 4  # Integer.BYTES
        self._txnum = page.get_int(txnum_position)

    def op(self) -> LogType:
        return LogType.COMMIT

    def tx_number(self) -> int:
        return self._txnum

    def undo(self, tx: Transaction) -> None:
        """Does nothing, because a commit record contains no undo information.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        pass

    def __str__(self) -> str:
        return f"<COMMIT {self._txnum}>"

    @staticmethod
    def write_to_log(log_manager: LogManager, txnum: int) -> int:
        """This log record contains the COMMIT operator, followed by the transaction id.

        Args:
            log_manager (LogManager): the log manager
            txnum (int): the transaction id

        Returns:
            int: the LSN of the last log value
        """
        integer_record_size = 4
        page = Page(2 * integer_record_size)
        page.set_int(0, LogType.COMMIT.value)
        page.set_int(integer_record_size, txnum)
        return log_manager.append(page.contents())


class RollbackRecord(LogRecord):
    """The ROLLBACK log record."""

    _txnum: int

    def __init__(self, page: Page) -> None:
        """Create a new rollback log record.

        Args:
            page (Page): the page containing the log values
        """
        txnum_position = 4  # Integer.BYTES
        self._txnum = page.get_int(txnum_position)

    def op(self) -> LogType:
        return LogType.ROLLBACK

    def tx_number(self) -> int:
        return self._txnum

    def undo(self, tx: Transaction) -> None:
        """Does nothing, because a rollback record contains no undo information.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        pass

    def __str__(self) -> str:
        return f"<ROLLBACK {self._txnum}>"

    @staticmethod
    def write_to_log(log_manager: LogManager, txnum: int) -> int:
        """A static method to write a rollback record to the log.
        This log record contains the ROLLBACK operator, followed by the transaction id.

        Args:
            log_manager (LogManager): the log manager
            txnum (int): the transaction id

        Returns:
            int: the LSN of the last log value
        """
        integer_record_size = 4
        page = Page(2 * integer_record_size)
        page.set_int(0, LogType.ROLLBACK.value)
        page.set_int(integer_record_size, txnum)
        return log_manager.append(page.contents())


class SetIntRecord(LogRecord):
    _txnum: int
    _offset: int
    _value: int
    _block_id: BlockId

    def __init__(self, page: Page) -> None:
        """Create a new setint log record.

        Args:
            page (Page): the page containing the log values.
            The format of the log record in the page is as follows:
                0: the transaction id
                4: filename
                4 + filename.length: block number
                8 + filename.length: offset
                12 + filename.length: the previous integer value
        """
        txnum_position = 4  # Integer.BYTES
        self._txnum = page.get_int(txnum_position)
        filename_position = txnum_position + 4  # Integer.BYTES
        filename = page.get_string(filename_position)
        block_number_position = filename_position + Page.max_length(len(filename))
        block_number = page.get_int(block_number_position)
        self._block_id = BlockId(filename, block_number)
        offset_position = block_number_position + 4  # Integer.BYTES
        self._offset = page.get_int(offset_position)
        value_position = offset_position + 4  # Integer.BYTES
        self._value = page.get_int(value_position)

    def op(self) -> LogType:
        return LogType.SETINT

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<SETINT {self._txnum} {self._block_id} {self._offset} {self._value}>"

    def undo(self, tx: Transaction) -> None:
        """Replace the specified data value with the value saved in the log record.
        The method pins a buffer to the specified block, calls setInt to restore the saved value, and unpins the buffer.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        tx.pin(self._block_id)
        tx.set_int(self._block_id, self._offset, self._value, False)
        tx.unpin(self._block_id)

    @staticmethod
    def write_to_log(log_manager: LogManager, txnum: int, block_id: BlockId, offset: int, value: int) -> int:
        """A static method to write a setInt record to the log.
        This log record contains the SETINT operator, followed by the transaction id, the filename, number,
        and offset of the modified block, and the previous integer value at that offset.

        Args:
            log_manager (LogManager): the log manager
            txnum (int): the transaction id
            block_id (BlockId): the block containing the value
            offset (int): the offset of the value
            value (int): the previous value

        Returns:
            int: the LSN of the last log value
        """
        txnum_position = 4  # Integer.BYTES
        filename_position = txnum_position + 4  # Integer.BYTES
        block_number_position = filename_position + Page.max_length(len(block_id.filename))
        offset_position = block_number_position + 4  # Integer.BYTES
        value_position = offset_position + 4  # Integer.BYTES
        record_size = value_position + 4  # Integer.BYTES
        page = Page(record_size)
        page.set_int(0, LogType.SETINT.value)
        page.set_int(txnum_position, txnum)
        page.set_string(filename_position, block_id.filename)
        page.set_int(block_number_position, block_id.block_number)
        page.set_int(offset_position, offset)
        page.set_int(value_position, value)
        return log_manager.append(page.contents())


class SetStringRecord(LogRecord):
    _txnum: int
    _offset: int
    _value: str
    _block_id: BlockId

    def __init__(self, page: Page) -> None:
        """Create a new setstring log record.

        Args:
            page (Page): the page containing the log values.
            The format of the log record in the page is as follows:
                0: the transaction id
                4: filename
                4 + filename.length: block number
                8 + filename.length: offset
                12 + filename.length: the previous string value
        """
        txnum_position = 4  # Integer.BYTES
        self._txnum = page.get_int(txnum_position)
        filename_position = txnum_position + 4
        filename = page.get_string(filename_position)
        block_number_position = filename_position + Page.max_length(len(filename))
        block_number = page.get_int(block_number_position)
        self._block_id = BlockId(filename, block_number)
        offset_position = block_number_position + 4  # Integer.BYTES
        self._offset = page.get_int(offset_position)
        value_position = offset_position + 4  # Integer.BYTES
        self._value = page.get_string(value_position)

    def op(self) -> LogType:
        return LogType.SETSTRING

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<SETSTRING {self._txnum} {self._block_id} {self._offset} {self._value}>"

    def undo(self, tx: Transaction) -> None:
        """Replace the specified data value with the value saved in the log record.
        The method pins a buffer to the specified block, calls setInt to restore the saved value, and unpins the buffer.

        Args:
            tx (Transaction): the transaction that the log record is undoing
        """
        tx.pin(self._block_id)
        tx.set_string(self._block_id, self._offset, self._value, False)
        tx.unpin(self._block_id)

    @staticmethod
    def write_to_log(log_manager: LogManager, txnum: int, block_id: BlockId, offset: int, value: str) -> int:
        """A static method to write a setInt record to the log.
        This log record contains the SETINT operator, followed by the transaction id, the filename, number,
        and offset of the modified block, and the previous integer value at that offset.

        Args:
            log_manager (LogManager): the log manager
            txnum (int): the transaction id
            block_id (BlockId): the block containing the value
            offset (int): the offset of the value
            value (str): the previous value

        Returns:
            int: the LSN of the last log value
        """
        txnum_position = 4  # Integer.BYTES
        filename_position = txnum_position + 4  # Integer.BYTES
        block_number_position = filename_position + Page.max_length(len(block_id.filename))
        offset_position = block_number_position + 4  # Integer.BYTES
        value_position = offset_position + 4  # Integer.BYTES
        record_size = value_position + Page.max_length(len(value))
        page = Page(record_size)
        page.set_int(0, LogType.SETSTRING.value)
        page.set_int(txnum_position, txnum)
        page.set_string(filename_position, block_id.filename)
        page.set_int(block_number_position, block_id.block_number)
        page.set_int(offset_position, offset)
        page.set_string(value_position, value)
        return log_manager.append(page.contents())


class RecoveryManager:
    """The recovery manager. Each transaction has its own recovery manager."""

    _log_manager: LogManager
    _buffer_manager: BufferManager
    _transaction: Transaction
    _txnum: int

    def __init__(self, tx: Transaction, txnum: int, log_manager: LogManager, buffer_manager: BufferManager) -> None:
        """Create a recovery manager for the specified transaction.

        Args:
            tx (Transaction): the transaction
            txnum (int): the id of the specified transaction
            log_manager (LogManager): the log manager
            buffer_manager (BufferManager): the buffer manager
        """
        self._transaction = tx
        self._txnum = txnum
        self._log_manager = log_manager
        self._buffer_manager = buffer_manager
        StartRecord.write_to_log(self._log_manager, self._txnum)

    def commit(self) -> None:
        """Write a commit record to the log, and flushes it to disk."""
        self._buffer_manager.flush_all(self._txnum)
        lsn = CommitRecord.write_to_log(self._log_manager, self._txnum)
        self._log_manager.flush(lsn)

    def rollback(self) -> None:
        """Write a rollback record to the log, and flushes it to disk."""
        self._do_rollback()
        self._buffer_manager.flush_all(self._txnum)
        lsn = RollbackRecord.write_to_log(self._log_manager, self._txnum)
        self._log_manager.flush(lsn)

    def recover(self) -> None:
        """Recover uncompleted transactions from the log and then
        write a quiescent checkpoint record to the log and flush it to disk."""

        self._do_recover()
        self._buffer_manager.flush_all(self._txnum)
        lsn = CheckpointRecord.write_to_log(self._log_manager)
        self._log_manager.flush(lsn)

    def set_int(self, buffer: Buffer, offset: int, new_value: int) -> int:
        """Write a setint record to the log and return its lsn.

        Args:
            buffer (Buffer): the buffer containing the page
            offset (int): the ofset of the value in the page
            new_value (int): the value to be written

        Returns:
            int: the LSN of the last log value
        """
        old_value = buffer.contents.get_int(offset)
        block_id = buffer.block_id
        assert block_id is not None
        lsn = SetIntRecord.write_to_log(self._log_manager, self._txnum, block_id, offset, old_value)
        return lsn

    def set_string(self, buffer: Buffer, offset: int, new_value: str) -> int:
        """Write a setstring record to the log and return its lsn.

        Args:
            buffer (Buffer): the buffer containing the page
            offset (int): the ofset of the value in the page
            new_value (str): the value to be written

        Returns:
            int: the LSN of the last log value
        """
        old_value = buffer.contents.get_string(offset)
        block_id = buffer.block_id
        assert block_id is not None
        lsn = SetStringRecord.write_to_log(self._log_manager, self._txnum, block_id, offset, old_value)
        return lsn

    def _do_rollback(self) -> None:
        """Rollback the transaction, by iterating through the log records until it finds the transaction's START record,
        calling undo() for each of the transaction's log records.
        """
        for b in self._log_manager:
            log_record = LogRecord.create_log_record(b)
            assert log_record is not None
            if log_record.tx_number() == self._txnum:
                if log_record.op() == LogType.START:
                    return
                log_record.undo(self._transaction)

    def _do_recover(self) -> None:
        """Do a complete database recovery. The method iterates through the log records.
        Whenever it finds a log record for an unfinished transaction, it calls undo() on that record.
        The method stops when it encounters a CHECKPOINT record or the end of the log.
        """
        finished_txs = []
        for b in self._log_manager:
            log_record = LogRecord.create_log_record(b)
            assert log_record is not None
            if log_record.op() == LogType.CHECKPOINT:
                return
            if log_record.op() == LogType.COMMIT or log_record.op() == LogType.ROLLBACK:
                finished_txs.append(log_record.tx_number())
            elif log_record.tx_number() not in finished_txs:
                log_record.undo(self._transaction)
