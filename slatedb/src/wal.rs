use std::sync::Arc;

use async_trait::async_trait;

use crate::block::{Block, BlockBuilder};
use crate::block_iterator::BlockIterator;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};

pub(crate) struct WalBuffer {
    block_builder: BlockBuilder,
    table_store: Arc<TableStore>,
    wal_id: u64,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
}

impl WalBuffer {
    pub(crate) fn new(table_store: Arc<TableStore>, wal_id: u64) -> Self {
        Self {
            block_builder: BlockBuilder::new(usize::MAX),
            table_store,
            wal_id,
            durable: WatchableOnceCell::new(),
        }
    }

    pub(crate) fn append(
        &mut self,
        entries: &[RowEntry],
    ) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        for entry in entries.iter() {
            // TODO: handle exceeding block size
            let _ = self.block_builder.add(entry.clone());
        }
        self.durable.reader()
    }

    pub(crate) async fn flush(self) -> Result<(), SlateDBError> {
        if self.block_builder.is_empty() {
            self.durable.write(Ok(()));
            return Ok(());
        }

        let block = self.block_builder.build()?;
        let data = block.encode();
        let result = self.table_store.write_wal_block(self.wal_id, data).await;
        self.durable.write(result.clone());
        result
    }
}

/// Iterator over entries in a WAL block file.
pub(crate) struct WalBlockIterator {
    inner: BlockIterator<Block>,
}

impl WalBlockIterator {
    /// Create a new iterator by reading the WAL block from object storage.
    pub(crate) async fn open(
        table_store: Arc<TableStore>,
        wal_id: u64,
    ) -> Result<Self, SlateDBError> {
        let bytes = table_store.read_wal_block(wal_id).await?;
        let block = Block::decode(bytes);
        let inner = BlockIterator::new_ascending(block);
        Ok(Self { inner })
    }
}

#[async_trait]
impl KeyValueIterator for WalBlockIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.inner.init().await
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.inner.next_entry().await
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.inner.seek(next_key).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    use crate::iter::KeyValueIterator;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::tablestore::TableStore;
    use crate::types::{RowEntry, ValueDeletable};

    use super::{WalBlockIterator, WalBuffer};

    fn setup_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ))
    }

    #[tokio::test]
    async fn test_wal_buffer_write_and_read() {
        let table_store = setup_table_store();
        let wal_id = 1;

        // Write entries using WalBuffer
        let mut wal_buffer = WalBuffer::new(table_store.clone(), wal_id);

        let entry1 = RowEntry::new_value(b"key1", b"value1", 1);
        let entry2 = RowEntry::new(
            Bytes::from("key2"),
            ValueDeletable::Value(Bytes::from("value2")),
            2,
            None,
            None,
        );
        let entry3 = RowEntry::new(
            Bytes::from("key3"),
            ValueDeletable::Value(Bytes::from("value3")),
            3,
            None,
            None,
        );

        wal_buffer.append(&[entry1.clone()]);
        wal_buffer.append(&[entry2.clone(), entry3.clone()]);
        wal_buffer.flush().await.unwrap();

        // Read entries using WalBlockIterator
        let mut iter = WalBlockIterator::open(table_store.clone(), wal_id)
            .await
            .unwrap();

        let read_entry1 = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(read_entry1.key, entry1.key);
        assert_eq!(read_entry1.value, entry1.value);
        assert_eq!(read_entry1.seq, entry1.seq);

        let read_entry2 = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(read_entry2.key, entry2.key);
        assert_eq!(read_entry2.value, entry2.value);
        assert_eq!(read_entry2.seq, entry2.seq);

        let read_entry3 = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(read_entry3.key, entry3.key);
        assert_eq!(read_entry3.value, entry3.value);
        assert_eq!(read_entry3.seq, entry3.seq);

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wal_buffer_empty_flush() {
        let table_store = setup_table_store();
        let wal_id = 2;

        // Flush without appending any entries
        let wal_buffer = WalBuffer::new(table_store.clone(), wal_id);
        wal_buffer.flush().await.unwrap();

        // Reading should fail since no file was written
        let result = WalBlockIterator::open(table_store.clone(), wal_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wal_buffer_durable_notification() {
        let table_store = setup_table_store();
        let wal_id = 3;

        let mut wal_buffer = WalBuffer::new(table_store.clone(), wal_id);

        let entry = RowEntry::new(
            Bytes::from("key"),
            ValueDeletable::Value(Bytes::from("value")),
            1,
            None,
            None,
        );

        let mut durable_reader = wal_buffer.append(&[entry]);
        wal_buffer.flush().await.unwrap();

        // The durable reader should now have the result
        let result = durable_reader.await_value().await;
        assert!(result.is_ok());
    }
}