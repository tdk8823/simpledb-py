from simpledbpy.file import BlockId, FileManager, Page
from simpledbpy.log import LogManager


class Buffer:
    _file_manager: FileManager
    _log_manager: LogManager
    _contents: Page
    _block_id: BlockId
    _pins: int
    _txnum: int
    _lsn: int

    def __init__(self, file_manager: FileManager, log_manager: LogManager):
        self._file_manager = file_manager
        self._log_manager = log_manager
        self._contents = Page(file_manager.block_size)

    @property
    def contents(self) -> Page:
        return self._contents

    @property
    def block_id(self) -> BlockId:
        return self._block_id

    def set_modified(self, txnum: int, lsn: int) -> None:
        self._txnum = txnum
        if lsn >= 0:
            self._lsn = lsn

    def is_pinned(self) -> bool:
        return self._pins > 0

    @property
    def modifying_txnum(self) -> int:
        return self._txnum

    def assign_to_block(self, block_id: BlockId) -> None:
        self.flush()
        self._block_id = block_id
        self._file_manager.read(self._block_id, self._contents)
        self._pins = 0

    def flush(self) -> None:
        if self._txnum >= 0:
            self._log_manager.flush(self._lsn)
            self._file_manager.write(self._block_id, self._contents)
            self._txnum = -1

    def pin(self) -> None:
        self._pins += 1

    def unpin(self) -> None:
        self._pins -= 1
