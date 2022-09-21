//! Tool to clean up old object store files that don't appear in the catalog.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]
#![allow(clippy::missing_docs_in_private_items)]

use crate::{
    objectstore::{checker as os_checker, deleter as os_deleter, lister as os_lister},
    parquetfile::deleter as pf_deleter,
};

use clap::Parser;
use humantime::{format_duration, parse_duration};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Logic for listing, checking and deleting files in object storage
mod objectstore;
/// Logic deleting parquet files from the catalog
mod parquetfile;

const BUFFER_SIZE: usize = 1000;

/// Run the tasks that clean up old object store files that don't appear in the catalog.
pub async fn main(config: Config) -> Result<()> {
    GarbageCollector::start(config)?.join().await
}

/// The tasks that clean up old object store files that don't appear in the catalog.
pub struct GarbageCollector {
    shutdown: CancellationToken,
    os_lister: tokio::task::JoinHandle<Result<(), os_lister::Error>>,
    os_checker: tokio::task::JoinHandle<Result<(), os_checker::Error>>,
    os_deleter: tokio::task::JoinHandle<Result<(), os_deleter::Error>>,
    pf_deleter: tokio::task::JoinHandle<Result<(), pf_deleter::Error>>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish_non_exhaustive()
    }
}

impl GarbageCollector {
    /// Construct the garbage collector and start it
    pub fn start(config: Config) -> Result<Self> {
        let Config {
            object_store,
            sub_config,
            catalog,
        } = config;

        let dry_run = sub_config.dry_run;
        info!(
            objectstore_cutoff_days = %format_duration(sub_config.objectstore_cutoff).to_string(),
            parquetfile_cutoff_days = %format_duration(sub_config.parquetfile_cutoff).to_string(),
            objectstore_sleep_interval_minutes = %sub_config.objectstore_sleep_interval_minutes,
            parquetfile_sleep_interval_minutes = %sub_config.parquetfile_sleep_interval_minutes,
            "GarbageCollector starting"
        );

        // Shutdown handler channel to notify children
        let shutdown = CancellationToken::new();

        // Initialise the object store garbage collector, which works as three communicating threads:
        // - lister lists objects in the object store and sends them on a channel. the lister will
        //   run until it has enumerated all matching files, then sleep for the configured
        //   interval.
        // - checker receives from that channel and checks the catalog to see if they exist, if not
        //   it sends them on another channel
        // - deleter receives object store entries that have been checked and therefore should be
        //   deleted.
        let (tx1, rx1) = mpsc::channel(BUFFER_SIZE);
        let (tx2, rx2) = mpsc::channel(BUFFER_SIZE);

        let os_lister = tokio::spawn(os_lister::perform(
            shutdown.clone(),
            Arc::clone(&object_store),
            tx1,
            sub_config.objectstore_sleep_interval_minutes,
        ));
        let os_checker = tokio::spawn(os_checker::perform(
            Arc::clone(&catalog),
            chrono::Duration::from_std(sub_config.objectstore_cutoff).map_err(|e| {
                Error::CutoffError {
                    message: e.to_string(),
                }
            })?,
            rx1,
            tx2,
        ));
        let os_deleter = tokio::spawn(os_deleter::perform(
            object_store,
            dry_run,
            sub_config.objectstore_concurrent_deletes,
            rx2,
        ));

        // Initialise the parquet file deleter, which is just one thread that calls delete_old()
        // on the catalog then sleeps.
        let pf_deleter = tokio::spawn(pf_deleter::perform(
            shutdown.clone(),
            catalog,
            sub_config.parquetfile_cutoff,
            sub_config.parquetfile_sleep_interval_minutes,
        ));

        Ok(Self {
            shutdown,
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
        })
    }

    /// A handle to gracefully shutdown the garbage collector when invoked
    pub fn shutdown_handle(&self) -> impl Fn() {
        let shutdown = self.shutdown.clone();
        move || {
            shutdown.cancel();
        }
    }

    /// Wait for the garbage collector to finish work
    pub async fn join(self) -> Result<()> {
        let Self {
            os_lister,
            os_checker,
            os_deleter,
            pf_deleter,
            shutdown: _,
        } = self;

        let (os_lister, os_checker, os_deleter, pf_deleter) =
            futures::join!(os_lister, os_checker, os_deleter, pf_deleter);

        pf_deleter.context(ParquetFileDeleterPanicSnafu)??;
        os_deleter.context(ObjectStoreDeleterPanicSnafu)??;
        os_checker.context(ObjectStoreCheckerPanicSnafu)??;
        os_lister.context(ObjectStoreListerPanicSnafu)??;

        Ok(())
    }
}

/// Configuration to run the object store garbage collector
#[derive(Clone)]
pub struct Config {
    /// The object store to garbage collect
    pub object_store: Arc<DynObjectStore>,

    /// The catalog to check if an object is garbage
    pub catalog: Arc<dyn Catalog>,

    /// The garbage collector specific configuration
    pub sub_config: SubConfig,
}

impl Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config (GarbageCollector")
            .field("sub_config", &self.sub_config)
            .finish_non_exhaustive()
    }
}

/// Configuration specific to the object store garbage collector
#[derive(Debug, Clone, Parser)]
pub struct SubConfig {
    /// If this flag is specified, don't delete the files in object storage. Only print the files
    /// that would be deleted if this flag wasn't specified.
    #[clap(long, env = "INFLUXDB_IOX_GC_DRY_RUN")]
    dry_run: bool,

    /// Items in the object store that are older than this duration.
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        parse(try_from_str = parse_duration),
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF"
    )]
    objectstore_cutoff: Duration,

    /// Number of concurrent object store deletion tasks
    #[clap(
        long,
        default_value_t = 5,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_CONCURRENT_DELETES"
    )]
    objectstore_concurrent_deletes: usize,

    /// Number of minutes to sleep between iterations of the objectstore deletion loop.
    /// Defaults to 30 minutes.
    #[clap(
        long,
        default_value_t = 30,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_SLEEP_INTERVAL_MINUTES"
    )]
    objectstore_sleep_interval_minutes: u64,

    /// Parquet file rows in the catalog flagged for deletion before this many days ago will be
    /// deleted.
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        parse(try_from_str = parse_duration),
        env = "INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF"
    )]
    parquetfile_cutoff: Duration,

    /// Number of minutes to sleep between iterations of the parquet file deletion loop.
    /// Defaults to 30 minutes.
    #[clap(
        long,
        default_value_t = 30,
        env = "INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES"
    )]
    parquetfile_sleep_interval_minutes: u64,
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error converting parsed duration: {message}"))]
    CutoffError { message: String },

    #[snafu(display("The object store lister task failed"))]
    #[snafu(context(false))]
    ObjectStoreLister { source: os_lister::Error },
    #[snafu(display("The object store lister task panicked"))]
    ObjectStoreListerPanic { source: tokio::task::JoinError },

    #[snafu(display("The object store checker task failed"))]
    #[snafu(context(false))]
    ObjectStoreChecker { source: os_checker::Error },
    #[snafu(display("The object store checker task panicked"))]
    ObjectStoreCheckerPanic { source: tokio::task::JoinError },

    #[snafu(display("The object store deleter task failed"))]
    #[snafu(context(false))]
    ObjectStoreDeleter { source: os_deleter::Error },
    #[snafu(display("The object store deleter task panicked"))]
    ObjectStoreDeleterPanic { source: tokio::task::JoinError },

    #[snafu(display("The parquet file deleter task failed"))]
    #[snafu(context(false))]
    ParquetFileDeleter { source: pf_deleter::Error },
    #[snafu(display("The parquet file deleter task panicked"))]
    ParquetFileDeleterPanic { source: tokio::task::JoinError },
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use clap_blocks::{
        catalog_dsn::CatalogDsnConfig,
        object_store::{make_object_store, ObjectStoreConfig},
    };
    use filetime::FileTime;
    use std::{fs, iter, path::PathBuf};
    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn deletes_untracked_files_older_than_the_cutoff() {
        let setup = OldFileSetup::new();

        let config = build_config(setup.data_dir_arg(), []).await;
        tokio::spawn(async {
            main(config).await.unwrap();
        });

        // file-based objectstore only has one file, it can't take long
        sleep(Duration::from_millis(500)).await;

        assert!(
            !setup.file_path.exists(),
            "The path {} should have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    #[tokio::test]
    async fn preserves_untracked_files_newer_than_the_cutoff() {
        let setup = OldFileSetup::new();

        #[rustfmt::skip]
        let config = build_config(setup.data_dir_arg(), [
            "--objectstore-cutoff", "10y",
        ]).await;
        tokio::spawn(async {
            main(config).await.unwrap();
        });

        // file-based objectstore only has one file, it can't take long
        sleep(Duration::from_millis(500)).await;

        assert!(
            setup.file_path.exists(),
            "The path {} should not have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    async fn build_config(data_dir: &str, args: impl IntoIterator<Item = &str> + Send) -> Config {
        let sub_config = SubConfig::parse_from(iter::once("dummy-program-name").chain(args));
        let object_store = object_store(data_dir);
        let catalog = catalog().await;

        Config {
            object_store,
            catalog,
            sub_config,
        }
    }

    fn object_store(data_dir: &str) -> Arc<DynObjectStore> {
        #[rustfmt::skip]
        let cfg = ObjectStoreConfig::parse_from([
            "dummy-program-name",
            "--object-store", "file",
            "--data-dir", data_dir,
        ]);
        make_object_store(&cfg).unwrap()
    }

    async fn catalog() -> Arc<dyn Catalog> {
        #[rustfmt::skip]
        let cfg = CatalogDsnConfig::parse_from([
            "dummy-program-name",
            "--catalog", "memory",
        ]);

        let metrics = metric::Registry::default().into();

        cfg.get_catalog("garbage_collector", metrics).await.unwrap()
    }

    struct OldFileSetup {
        data_dir: TempDir,
        file_path: PathBuf,
    }

    impl OldFileSetup {
        const APRIL_9_2018: FileTime = FileTime::from_unix_time(1523308536, 0);

        fn new() -> Self {
            let data_dir = TempDir::new().unwrap();

            let file_path = data_dir.path().join("some-old-file");
            fs::write(&file_path, "dummy content").unwrap();
            filetime::set_file_mtime(&file_path, Self::APRIL_9_2018).unwrap();

            Self {
                data_dir,
                file_path,
            }
        }

        fn data_dir_arg(&self) -> &str {
            self.data_dir.path().to_str().unwrap()
        }
    }
}
