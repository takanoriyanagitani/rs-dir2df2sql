use std::env;
use std::io;
use std::process::ExitCode;

use rs_dir2df2sql::datafusion;

use datafusion::prelude::DataFrame;
use datafusion::prelude::SessionContext;

fn env2dirname() -> Result<String, io::Error> {
    env::var("ENV_INPUT_DIR_NAME").map_err(io::Error::other)
}

fn env2table_name() -> String {
    env::var("ENV_TABLE_NAME").unwrap_or_else(|_| "the_dir".into())
}

fn arg2sql() -> Result<String, io::Error> {
    let mut args = env::args();
    args.next();

    match args.next() {
        Some(sql) => Ok(sql),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "SQL statement missing. Provide it as the first argument.",
        )),
    }
}

async fn show_df(df: DataFrame) -> Result<(), io::Error> {
    df.show().await.map_err(io::Error::other)
}

async fn sub() -> Result<(), io::Error> {
    let ctx = SessionContext::new();
    let dirname: String = env2dirname()?;
    let tabname: String = env2table_name();
    rs_dir2df2sql::register_dirents_in_dir_as_batch(&ctx, &tabname, dirname, None).await?;
    let query: String = arg2sql()?;
    let df = ctx.sql(&query).await?;
    show_df(df).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
