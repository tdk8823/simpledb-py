import time
from threading import Condition
from typing import Dict

from simpledbpy.file import BlockId


class LockAbortError(RuntimeError):
    pass


class LockTable:
    """The lock table, which provides methods to lock and unlock blocks.
    If a transaction requests a lock that causes a conflict with an existing lock,
    then that transaction is placed on a wait list. There is only one wait list for all blocks.
    When the last lock on a block is unlocked, then all transactions are removed from the wait list and rescheduled.
    If one of those transactions discovers that the lock it is waiting for is still locked,
    it will place itself back on the wait list.
    """

    MAX_TIME = 10

    _locks: Dict[BlockId, int]
    _cv: Condition

    def __init__(self) -> None:
        self._locks = {}
        self._cv = Condition()

    def slock(self, block_id: BlockId) -> None:
        """Grant an SLock on the specified block. If an XLock exists when the method is called,
        then the calling thread will be placed on a wait list until the lock is released.
        If the thread remains on the wait list for a certain amount of time (currently 10 seconds),
        then an exception is thrown.

        Args:
            block_id (BlockId): The block to be locked.

        Raises:
            LockAbortError: If the lock cannot be granted.
        """
        with self._cv:
            try:
                timestamp = time.time()
                while self._has_xlock(block_id) and not self._waiting_too_long(timestamp):
                    self._cv.wait(self.MAX_TIME)
                if self._has_xlock(block_id):
                    raise LockAbortError()
                val = self._get_lock_val(block_id)
                self._locks[block_id] = val + 1
            except InterruptedError:
                raise LockAbortError()

    def xlock(self, block_id: BlockId) -> None:
        """Grant an XLock on the specified block.
        If a lock of any type exists when the method is called, then the calling thread will be placed on
        a wait list until the locks are released. If the thread remains on the wait list for a certain
        amount of time(currently 10 seconds), then an exception is thrown.

        Args:
            block_id (BlockId): The block to be locked.

        Raises:
            LockAbortError: If the lock cannot be granted.
        """
        with self._cv:
            try:
                timestamp = time.time()
                while self._has_other_slocks(block_id) and not self._waiting_too_long(timestamp):
                    self._cv.wait(self.MAX_TIME)
                if self._has_other_slocks(block_id):
                    raise LockAbortError()
                self._locks[block_id] = -1
            except InterruptedError:
                raise LockAbortError()

    def unlock(self, block_id: BlockId) -> None:
        """Release a lock on the specified block.
        If this lock is the last lock on that block, then the waiting transactions are notified.

        Args:
            block_id (BlockId): The block to be unlocked.
        """
        val = self._get_lock_val(block_id)
        if val > 1:
            self._locks[block_id] = val - 1
        else:
            self._locks.pop(block_id)
            with self._cv:
                self._cv.notify_all()

    def _has_xlock(self, block_id: BlockId) -> bool:
        return self._get_lock_val(block_id) < 0

    def _has_other_slocks(self, block_id: BlockId) -> bool:
        return self._get_lock_val(block_id) > 1

    def _waiting_too_long(self, start_time: float) -> bool:
        return time.time() - start_time > self.MAX_TIME

    def _get_lock_val(self, block_id: BlockId) -> int:
        ival = self._locks.get(block_id)
        if ival is None:
            return 0
        return ival


class ConcurrencyManager:
    """The concurrency manager for the transaction.
    Each transaction has its own concurrency manager. The concurrency manager keeps track of which locks the
    transaction currently has, and interacts with the global lock table as needed.

    Attributes:
        _lock_table (LockTable): The global lock table.
        This variable is static because all transactions share the same table.
        _locks (Dict[BlockId, str]): The locks held by the transaction.
    """

    _lock_table = LockTable()
    _locks: dict[BlockId, str]

    def __init__(self) -> None:
        self._locks = {}

    def slock(self, block_id: BlockId) -> None:
        """Obtain an SLock on the block, if necessary.
        The method will ask the lock table for an SLock if the transaction currently has no locks on that block.

        Args:
            block_id (BlockId): The block to be locked.
        """
        if block_id not in self._locks:
            self._lock_table.slock(block_id)
            self._locks[block_id] = "S"

    def xlock(self, block_id: BlockId) -> None:
        """Obtain an XLock on the block, if necessary.
        If the transaction does not have an XLock on that block, then the method first gets an SLock on that block
        (if necessary), and then upgrades it to an XLock.

        Args:
            block_id (BlockId): _description_
        """
        if not self._has_xlock(block_id):
            self.slock(block_id)
            self._lock_table.xlock(block_id)
            self._locks[block_id] = "X"

    def release(self) -> None:
        for block_id in self._locks:
            self._lock_table.unlock(block_id)
        self._locks.clear()

    def _has_xlock(self, block_id: BlockId) -> bool:
        lock_type = self._locks.get(block_id)
        return lock_type is not None and lock_type == "X"
