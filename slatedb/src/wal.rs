use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::block::{Block, BlockBuilder, SIZEOF_U16};
use crate::block_iterator::BlockIterator;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};
use crate::wal_id::WalIdStore;

pub(crate) struct Wal {
    buffer: Buffer,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
    // ToDo: only store the WAL store in this struct
    table_store: Arc<TableStore>,
    max_in_memory_bytes_size: Option<usize>,
}

impl Wal {

    pub(crate) fn new(
        wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
        table_store: Arc<TableStore>,
        max_in_memory_bytes_size: Option<usize>,
    ) -> Self {
        Self {
            buffer: Buffer::new(),
            durable: WatchableOnceCell::new(),
            wal_id_incrementor,
            table_store,
            max_in_memory_bytes_size,
        }
    }

    pub(crate) async fn write(&mut self, records: &[Bytes]) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.buffer.append(records);
        if let Some(max_size) = self.max_in_memory_bytes_size {
            if self.buffer.size() >= max_size {
                self.flush().await;
            }
        }
        self.durable.reader()
    }

    pub(crate) async fn flush(&mut self) {
        if self.buffer.is_empty() {
            self.durable.write(Ok(()));
            self.durable = WatchableOnceCell::new();
            return;
        }

        let encoded_buffer = self.buffer.encode();
        let wal_id = self.wal_id_incrementor.next_wal_id();
        let write_result = self.table_store.write_wal_block(wal_id, encoded_buffer).await;
        self.buffer.clear();
        self.durable.write(write_result.clone());
        self.durable = WatchableOnceCell::new();
    }
}

struct Buffer {
    offsets: Vec<u16>,
    records: Vec<Bytes>,
}

impl Buffer {

    pub(crate) fn new() -> Self {
        Self {
            offsets: Vec::new(),
            records: Vec::new(),
        }
    }

    pub(crate) fn append(&mut self, records: &[Bytes]) {
        let mut offset: usize = self.records.iter().map(|r| r.len()).sum();
        for record in records {
            self.offsets.push(offset as u16);
            offset += record.len();
            self.records.push(record.clone());
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn size(&self) -> usize {
        self.records.iter().map(|r| r.len()).sum()
    }

    pub(crate) fn encode(&mut self) -> Bytes {
        let total_len: usize = self.records.iter().map(|b| b.len()).sum();
        let mut buf = BytesMut::with_capacity(total_len);
        for record in &self.records {
            buf.extend_from_slice(record);
        }
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.freeze()
    }

    pub(crate) fn clear(&mut self) {
        self.offsets.clear();
        self.records.clear();
    }
}


pub(crate) struct WalObjectIterator {
    offsets: Vec<u16>,
    records: Bytes,
    current_offset: usize,
}

impl WalObjectIterator {
    fn new(wal_object: Bytes) -> Self{
        let data = wal_object.as_ref();
        let offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let records_len = data.len()
            - SIZEOF_U16
            - offsets_len * SIZEOF_U16;
        let offsets_raw = &data[records_len..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let records = wal_object.slice(..records_len);
        Self {
            offsets,
            records,
            current_offset: 0,
        }
    }
}

impl Iterator for WalObjectIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset < self.offsets.len() {
            let start = self.offsets[self.current_offset] as usize;
            self.current_offset += 1;
            let end = if self.current_offset < self.offsets.len() {
                self.offsets[self.current_offset] as usize
            } else {
                self.records.len()
            };
            Some(self.records.slice(start..end))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::tablestore::TableStore;
    use crate::wal_id::WalIdStore;

    use super::{Wal, WalObjectIterator};

    struct MockWalIdStore {
        next_id: AtomicU64,
    }

    impl MockWalIdStore {
        fn new(start_id: u64) -> Self {
            Self {
                next_id: AtomicU64::new(start_id),
            }
        }
    }

    impl WalIdStore for MockWalIdStore {
        fn next_wal_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        }
    }

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
    async fn test_wal_write_and_read() {
        let table_store = setup_table_store();
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore::new(1));

        let mut wal = Wal::new(wal_id_store, table_store.clone(), None);

        let record1 = Bytes::from("record1");
        let record2 = Bytes::from("record2");
        let record3 = Bytes::from("record3");

        wal.write(&[record1.clone()]).await;
        wal.write(&[record2.clone(), record3.clone()]).await;
        wal.flush().await;

        // Read records using WalObjectIterator
        let wal_data = table_store.read_wal_block(1).await.unwrap();
        let mut iter = WalObjectIterator::new(wal_data);

        assert_eq!(iter.next(), Some(record1));
        assert_eq!(iter.next(), Some(record2));
        assert_eq!(iter.next(), Some(record3));
        assert_eq!(iter.next(), None);
    }

    #[tokio::test]
    async fn test_wal_empty_flush() {
        let table_store = setup_table_store();
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore::new(1));

        let mut wal = Wal::new(wal_id_store, table_store.clone(), None);
        wal.flush().await;

        // Reading should fail since no file was written (empty buffer)
        let result = table_store.read_wal_block(1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wal_durable_notification() {
        let table_store = setup_table_store();
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore::new(1));

        let mut wal = Wal::new(wal_id_store, table_store.clone(), None);

        let record = Bytes::from("test_record");

        let mut durable_reader = wal.write(&[record]).await;
        wal.flush().await;

        // The durable reader should now have the result
        let result = durable_reader.await_value().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wal_multiple_flushes() {
        let table_store = setup_table_store();
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore::new(1));

        let mut wal = Wal::new(wal_id_store, table_store.clone(), None);

        // First write and flush
        let record1 = Bytes::from("first");
        wal.write(&[record1.clone()]).await;
        wal.flush().await;

        // Second write and flush
        let record2 = Bytes::from("second");
        wal.write(&[record2.clone()]).await;
        wal.flush().await;

        // Verify first WAL file
        let wal_data1 = table_store.read_wal_block(1).await.unwrap();
        let mut iter1 = WalObjectIterator::new(wal_data1);
        assert_eq!(iter1.next(), Some(record1));
        assert_eq!(iter1.next(), None);

        // Verify second WAL file
        let wal_data2 = table_store.read_wal_block(2).await.unwrap();
        let mut iter2 = WalObjectIterator::new(wal_data2);
        assert_eq!(iter2.next(), Some(record2));
        assert_eq!(iter2.next(), None);
    }

    #[tokio::test]
    async fn test_wal_auto_flush_on_size_limit() {
        let table_store = setup_table_store();
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore::new(1));

        // Set max size to 10 bytes
        let mut wal = Wal::new(wal_id_store, table_store.clone(), Some(10));

        // Write a small record (7 bytes) - should not trigger flush
        let record1 = Bytes::from("small");
        wal.write(&[record1.clone()]).await;

        // WAL file should not exist yet
        assert!(table_store.read_wal_block(1).await.is_err());

        // Write another record that pushes over the limit (5 + 6 = 11 bytes >= 10)
        let record2 = Bytes::from("bigger");
        wal.write(&[record2.clone()]).await;

        // WAL file should now exist due to auto-flush
        let wal_data = table_store.read_wal_block(1).await.unwrap();
        let mut iter = WalObjectIterator::new(wal_data);
        assert_eq!(iter.next(), Some(record1));
        assert_eq!(iter.next(), Some(record2));
        assert_eq!(iter.next(), None);
    }
}