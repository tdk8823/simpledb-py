from threading import Lock

from simpledbpy.file import BlockId, FileManager, Page


class LogIterator:
    _file_manager: FileManager
    _block_id: BlockId
    _page: Page
    _current_position: int
    _boundary: int

    def __init__(self, file_manager: FileManager, block_id: BlockId):
        """Create an iterator for the log file, positioned after the last log record.

        Args:
            file_manager (FileManager): The file manager
            block_id (BlockId): The block containing the log records
        """
        self._file_manager = file_manager
        self._block_id = block_id
        b = bytes(file_manager.block_size)
        self._page = Page(b)
        self._move_to_block(block_id)

    def __iter__(self) -> "LogIterator":
        return self

    def __next__(self) -> bytes:
        """Moves to the next log record in the block. If there are no more log records in the block,
        then move to the previous block and return the log record from there.

        Raises:
            StopIteration: If there are no more log records in the log file

        Returns:
            bytes: the next earliest log record
        """
        if self._has_next():
            if self._current_position == self._file_manager.block_size:
                self._block_id = BlockId(self._block_id.filename, self._block_id.block_number - 1)
                self._move_to_block(self._block_id)
            log_record = self._page.get_bytes(self._current_position)
            self._current_position += len(log_record) + 4  # Assuming `Integer.BYTES` is 4
            return log_record
        else:
            raise StopIteration

    def _has_next(self) -> bool:
        """Determines if the current log record is the earliest record in the log file.

        Returns:
            bool: True if there is an earlier log record
        """
        return self._current_position < self._file_manager.block_size or self._block_id.block_number > 0

    def _move_to_block(self, block_id: BlockId) -> None:
        """Moves to the specified log block and positions it at the first record in that block
        (i.e., the most recent one).

        Args:
            block_id (BlockId): The block containing the log records
        """
        self._file_manager.read(block_id, self._page)
        self._boundary = self._page.get_int(0)
        self._current_position = self._boundary


class LogManager:
    """
    The log manager is responsible for writing log records into a log file.
    The format of a log record is as follows:

    |------------------------------------------block---------------------------------------------|
    | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | ... | block_size - 1 |
    |--------------------------------------------------------------------------------------------|
    | 5 |               | 3 |  log n+1  | 4 |      log n        |................................|

    The first integer in the block is the pointer to the start of the last log record written (the "boundary").
    The log records are written right to left in the block. The size of the record is written before the bytes.
    If the block is full, a new block is appended to the end of the log file.
    """

    _file_manager: FileManager
    _log_file: str
    _log_page: Page
    _current_block: BlockId
    _latest_lsn: int
    _lagest_saved_lsn: int
    _lock: Lock

    def __init__(self, file_manager: FileManager, log_file: str):
        """Create the manager for the specified log file.
        If the log file does not yet exist, it is created with an empty first block.

        Args:
            file_manager (FileManager): The file manager
            log_file (str): The name of the log file
        """
        self._file_manager = file_manager
        self._log_file = log_file
        self._latest_lsn = 0
        self._lagest_saved_lsn = 0
        self._lock = Lock()

        b = bytes(file_manager.block_size)
        self._log_page = Page(b)
        log_size = file_manager.length(log_file)
        if log_size == 0:
            self._current_block = self._append_new_block()
        else:
            self._current_block = BlockId(log_file, log_size - 1)
            file_manager.read(self._current_block, self._log_page)

    def flush(self, lsn: int) -> None:
        """Ensures that the log record corresponding to the specified LSN has been written to disk.
        All earlier log records will also be written to disk.

        Args:
            lsn (int): The log sequence number of a log record
        """
        if lsn >= self._lagest_saved_lsn:
            self._flush()

    def __iter__(self) -> LogIterator:
        """Creates an iterator for the log file, positioned at the earliest log record

        Returns:
            LogIterator: the log iterator
        """
        self._flush()
        return LogIterator(self._file_manager, self._current_block)

    def append(self, log_record: bytes) -> int:
        """Appends a log record to the log buffer.
        The record consists of an arbitrary array of bytes.
        Log records are written right to left in the buffer.
        The size of the record is written before the bytes.
        The beginning of the buffer contains the location of the last-written record (the "boundary").
        Storing the records backwards makes it easy to read them in reverse order.

        Args:
            log_record (bytes): a byte buffer containing the bytes.

        Returns:
            int: the LSN of the final value
        """
        with self._lock:
            boundary = self._log_page.get_int(0)
            record_size = len(log_record)
            bytes_needed = record_size + 4  # Assuming `Integer.BYTES` is 4
            if boundary - bytes_needed < 4:
                self._flush()
                self._current_block = self._append_new_block()
                boundary = self._log_page.get_int(0)
            record_position = boundary - bytes_needed
            self._log_page.set_bytes(record_position, log_record)
            self._log_page.set_int(0, record_position)
            self._latest_lsn += 1
            return self._latest_lsn

    def _append_new_block(self) -> BlockId:
        """Initialize the bytebuffer and append it to the log file.

        Returns:
            BlockId: the block id of the new block
        """
        block_id = self._file_manager.append(self._log_file)
        self._log_page.set_int(0, self._file_manager.block_size)
        self._file_manager.write(block_id, self._log_page)
        return block_id

    def _flush(self) -> None:
        """Write the buffer to the log file."""
        self._file_manager.write(self._current_block, self._log_page)
        self._lagest_saved_lsn = self._latest_lsn
