from abc import ABC, abstractmethod
from typing import Optional

from simpledbpy.query import Constant
from simpledbpy.record import RID, Layout, TableScan
from simpledbpy.tx.transaction import Transaction


class Index(ABC):
    """This interface contains methods to traverse an index."""

    @abstractmethod
    def before_first(self, search_key: Constant) -> None:
        """Positions the index before the first index record having the specified search key.

        Args:
            search_key (Constant): the search key value.
        """

        raise NotImplementedError

    @abstractmethod
    def next(self) -> bool:
        """Moves the index to the next record having the search key specified in the beforeFirst method.
        Returns false if there are no more such index records.

        Returns:
            bool: False if no other index records have the search key.
        """

        raise NotImplementedError

    @abstractmethod
    def get_data_rid(self) -> RID:
        """Returns the dataRID value stored in the current index record.

        Returns:
            RID: the data RID stored in the current index record.
        """

        raise NotImplementedError

    @abstractmethod
    def insert(self, data_value: Constant, data_rid: RID) -> None:
        """Inserts an index record having the specified dataval and dataRID values.

        Args:
            data_value (Constant): the data value in the new index record.
            data_rid (RID): the data RID in the new index record.
        """

        raise NotImplementedError

    @abstractmethod
    def delete(self, data_value: Constant, data_rid: RID) -> None:
        """Deletes the index record having the specified dataval and dataRID values.

        Args:
            data_value (Constant): the data value of the deleted index record.
            data_rid (RID): the data RID of the deleted index record.
        """

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Closes the index."""

        raise NotImplementedError


class HashIndex(Index):
    """A static hash implementation of the Index interface. A fixed number of buckets is allocated (currently, 100),
    and each bucket is implemented as a file of index records.
    """

    NUM_BUCKETS = 100
    _tx: Transaction
    _index_name: str
    _layout: Layout
    _search_key: Optional[Constant]
    _table_scan: Optional[TableScan]

    def __init__(self, tx: Transaction, index_name: str, layout: Layout) -> None:
        """Opens a hash index for the specified index.

        Args:
            tx (Transaction): the calling transaction
            index_name (str): the name of the index
            layout (Layout): the layout fo the index records
        """

        self._tx = tx
        self._index_name = index_name
        self._layout = layout
        self._search_key = None
        self._table_scan = None

    def before_first(self, search_key: Constant) -> None:
        """Positions the index before the first index record having the specified search key. The method hashes the
        search key to determine the bucket, and then opens a table scan on the file corresponding to the bucket.
        The table scan for the previous bucket (if any) is closed.

        Args:
            search_key (Constant): the search key value.
        """
        self.close()
        self._search_key = search_key
        bucket = hash(search_key) % self.NUM_BUCKETS
        table_name = f"{self._index_name}{bucket}"
        self._table_scan = TableScan(self._tx, table_name, self._layout)

    def next(self) -> bool:
        """Moves to the next record having the search key. The method loops through the table scan for the bucket,
        looking for a matching record, and returning false if there are no more such records.

        Returns:
            bool: False if no other index records have the search key.
        """
        assert self._table_scan is not None
        while self._table_scan.next():
            if self._table_scan.get_val("dataval") == self._search_key:
                return True
        return False

    def get_data_rid(self) -> RID:
        """Retrieves the dataRID from the current record in the table scan for the bucket.

        Returns:
            RID: the data RID stored in the current index record.
        """
        assert self._table_scan is not None
        block_num = self._table_scan.get_int("block")
        id = self._table_scan.get_int("id")
        return RID(block_num, id)

    def insert(self, data_value: Constant, data_rid: RID) -> None:
        """Inserts a new record into the table scan for the bucket.

        Args:
            data_value (Constant): the data value in the new index record.
            data_rid (RID): the data RID in the new index record.
        """
        self.before_first(data_value)
        assert self._table_scan is not None
        self._table_scan.insert()
        self._table_scan.set_int("block", data_rid.block_number)
        self._table_scan.set_int("id", data_rid.slot)
        self._table_scan.set_value("dataval", data_value)

    def delete(self, data_value: Constant, data_rid: RID) -> None:
        """Deletes the specified record from the table scan for the bucket. The method starts at the beginning of
        the scan, and loops through the records until the specified record is found.

        Args:
            data_value (Constant): the data value of the deleted index record.
            data_rid (RID): the data RID of the deleted index record.
        """
        self.before_first(data_value)
        assert self._table_scan is not None
        while self._table_scan.next():
            if self.get_data_rid() == data_rid:
                self._table_scan.delete()
                return

    def close(self) -> None:
        """Closes the index by closing the current table scan."""

        if self._table_scan is not None:
            self._table_scan.close()
        self._table_scan = None

    @staticmethod
    def search_cost(num_blocks: int, records_per_block: int) -> int:
        """Returns the cost of searching an index file having the specified number of blocks. The method assumes
        that all buckets are about the same size, and so the cost is simply the size of the bucket.

        Args:
            num_blocks (int): the number of blocks of index records
            records_per_block (int): the number of index records per block

        Returns:
            int: the cost of traversing the index
        """
        return num_blocks // HashIndex.NUM_BUCKETS
