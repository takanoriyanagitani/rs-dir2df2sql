pub use datafusion;

use std::io;
use std::path::Path;

use datafusion::common::arrow;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rs_dirents2meta2rbat::futures;

use futures::Stream;
use futures::StreamExt;

use datafusion::prelude::SessionContext;

pub fn register_batch(
    ctx: &SessionContext,
    table_name: &str,
    b: RecordBatch,
) -> Result<(), io::Error> {
    ctx.register_batch(table_name, b)
        .map(|_| ())
        .map_err(io::Error::other)
}

pub async fn filenames2batch<S>(
    names: S,
    schema: Option<SchemaRef>,
) -> Result<RecordBatch, io::Error>
where
    S: Stream<Item = Result<String, io::Error>> + Unpin,
{
    rs_dirents2meta2rbat::filenames2batch(names, schema).await
}

pub async fn register_filenames_as_batch<S>(
    ctx: &SessionContext,
    table_name: &str,
    names: S,
    schema: Option<SchemaRef>,
) -> Result<(), io::Error>
where
    S: Stream<Item = Result<String, io::Error>> + Unpin,
{
    let batch = filenames2batch(names, schema).await?;
    register_batch(ctx, table_name, batch)
}

pub async fn dir2filenames<P>(
    dirname: P,
) -> Result<impl Stream<Item = Result<String, io::Error>> + Unpin, io::Error>
where
    P: AsRef<Path>,
{
    let dpath: &Path = dirname.as_ref();
    let rdir = tokio::fs::read_dir(dpath)
        .await
        .map_err(|e| format!("unable to read the dir {dpath:#?}: {e}"))
        .map_err(io::Error::other)?;
    let dirents = tokio_stream::wrappers::ReadDirStream::new(rdir);
    Ok(dirents.map(|rdirent| {
        rdirent.map(|dirent| {
            dirent
                .path()
                .into_os_string()
                .into_string()
                .unwrap_or_default()
        })
    }))
}

/// Registers the dirents of the specified dir as a record batch.
pub async fn register_dirents_in_dir_as_batch<P>(
    ctx: &SessionContext,
    table_name: &str,
    dirname: P,
    schema: Option<SchemaRef>,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let names = dir2filenames(dirname).await?;
    register_filenames_as_batch(ctx, table_name, names, schema).await
}
