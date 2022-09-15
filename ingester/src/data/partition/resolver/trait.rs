use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{PartitionKey, ShardId, TableId};

use crate::data::partition::PartitionData;

/// An infallible resolver of [`PartitionData`] for the specified shard, table,
/// and partition key, returning an initialised [`PartitionData`] buffer for it.
#[async_trait]
pub(crate) trait PartitionProvider: Send + Sync + Debug {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> PartitionData;
}

#[async_trait]
impl<T> PartitionProvider for Arc<T>
where
    T: PartitionProvider,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> PartitionData {
        (**self)
            .get_partition(partition_key, shard_id, table_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use crate::data::partition::resolver::MockPartitionProvider;

    use super::*;

    #[tokio::test]
    async fn test_arc_impl() {
        let key = PartitionKey::from("bananas");
        let shard = ShardId::new(42);
        let table = TableId::new(24);
        let partition = PartitionId::new(4242);
        let data = PartitionData::new(partition, None);

        let mock = Arc::new(MockPartitionProvider::default().with_partition(
            key.clone(),
            shard,
            table,
            data,
        ));

        let got = mock.get_partition(key, shard, table).await;
        assert_eq!(got.id(), partition);
    }
}
