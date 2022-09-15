//! A [`PartitionProvider`] implementation that hits the [`Catalog`] to resolve
//! the partition id and persist offset.

use std::sync::Arc;

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Partition, PartitionKey, ShardId, TableId};
use iox_catalog::interface::Catalog;

use crate::data::partition::PartitionData;

use super::r#trait::PartitionProvider;

/// A [`PartitionProvider`] implementation that hits the [`Catalog`] to resolve
/// the partition id and persist offset, returning an initialised
/// [`PartitionData`].
#[derive(Debug)]
pub(crate) struct CatalogPartitionResolver {
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
}

impl CatalogPartitionResolver {
    /// Construct a [`CatalogPartitionResolver`] that looks up partitions in
    /// `catalog`.
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            backoff_config: Default::default(),
        }
    }

    async fn get(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> Result<Partition, iox_catalog::interface::Error> {
        self.catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(partition_key, shard_id, table_id)
            .await
    }
}

#[async_trait]
impl PartitionProvider for CatalogPartitionResolver {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> PartitionData {
        let p = Backoff::new(&self.backoff_config)
            .retry_all_errors("resolve namespace ID", || {
                self.get(partition_key.clone(), shard_id, table_id)
            })
            .await
            .expect("retry forever");

        PartitionData::new(p.id, p.persisted_sequence_number)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::ShardIndex;

    use super::*;

    const TABLE_NAME: &str = "bananas";
    const PARTITION_KEY: &str = "platanos";

    #[tokio::test]
    async fn test_resolver() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        let (shard_id, table_id) = {
            let mut repos = catalog.repositories().await;
            let t = repos.topics().create_or_get("platanos").await.unwrap();
            let q = repos.query_pools().create_or_get("platanos").await.unwrap();
            let ns = repos
                .namespaces()
                .create(
                    TABLE_NAME,
                    iox_catalog::INFINITE_RETENTION_POLICY,
                    t.id,
                    q.id,
                )
                .await
                .unwrap();

            let shard = repos
                .shards()
                .create_or_get(&t, ShardIndex::new(0))
                .await
                .unwrap();

            let table = repos
                .tables()
                .create_or_get(TABLE_NAME, ns.id)
                .await
                .unwrap();

            (shard.id, table.id)
        };

        let resolver = CatalogPartitionResolver::new(Arc::clone(&catalog));
        let got = resolver
            .get_partition(PartitionKey::from(PARTITION_KEY), shard_id, table_id)
            .await;

        let got = catalog
            .repositories()
            .await
            .partitions()
            .get_by_id(got.id)
            .await
            .unwrap()
            .expect("partition not created");
        assert_eq!(got.shard_id, shard_id);
        assert_eq!(got.table_id, table_id);
        assert_eq!(got.partition_key, PartitionKey::from(PARTITION_KEY));
    }
}
