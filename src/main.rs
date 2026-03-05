use chrono::{TimeZone, Utc};
use clap::Parser;
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use object_store::{
    ObjectStore,
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    path::Path,
};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use serde::Deserialize;
use serde_json::Value;
use stac::{
    Item,
    geoparquet::{Writer, WriterBuilder},
    hash::Hasher,
};
use std::{fs::File, path, sync::Arc};
use tokio::task::JoinSet;

const ACCOUNT: &str = "pcstacitems";
const CONTAINER_NAME: &str = "items";

#[derive(Parser)]
struct Args {
    prefix: String,
    year: String,
    outdir: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let sas_token = get_sas_token().await;
    let azure: Arc<dyn ObjectStore> = Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(ACCOUNT)
            .with_container_name(CONTAINER_NAME)
            .with_config(AzureConfigKey::SasKey, sas_token.token)
            .build()
            .unwrap(),
    );
    println!(
        "Searching {} for parquet files in {}",
        args.prefix, args.year
    );
    let mut list_stream = azure.list(Some(&args.prefix.into()));
    let mut metas = Vec::new();
    while let Some(meta) = list_stream.next().await.map(|result| result.unwrap()) {
        if contains_year(&meta.location, &args.year) {
            metas.push(meta);
        }
    }
    println!("Found {} paths", metas.len());
    let year: i32 = args.year.parse().unwrap();
    let hasher = Arc::new(
        Hasher::from_temporal_extent(
            Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap()
                ..Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).unwrap(),
        )
        .unwrap(),
    );
    std::fs::create_dir_all(&args.outdir).unwrap();
    let m = MultiProgress::new();
    let sty = ProgressStyle::with_template("{msg} [{bar:40}] {pos}/{len} rows ({eta})")
        .unwrap()
        .progress_chars("=> ");
    let mut set = JoinSet::new();
    for meta in metas {
        let azure = azure.clone();
        let hasher = hasher.clone();
        let outdir = args.outdir.clone();
        let pb = m.add(ProgressBar::new(0));
        pb.set_style(sty.clone());
        pb.set_message(meta.location.to_string());
        set.spawn(async move {
            let filename = meta
                .location
                .filename()
                .expect("meta location should have a filename");
            let outpath = path::Path::new(&outdir).join(filename);
            let reader =
                ParquetObjectReader::new(azure, meta.location.clone()).with_file_size(meta.size);
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
            let total_rows = builder.metadata().file_metadata().num_rows() as u64;
            pb.set_length(total_rows);
            let mut items: Vec<Item> = Vec::new();
            let mut stream = builder.build().unwrap();
            while let Some(record_batch) = stream.next().await.map(|result| result.unwrap()) {
                pb.inc(record_batch.num_rows() as u64);
                for mut item in stac::geoarrow::json::record_batch_to_json_rows(record_batch)
                    .unwrap()
                    .into_iter()
                    .map(|json| serde_json::from_value::<Item>(Value::Object(json)).unwrap())
                {
                    let hash = item.hash(&hasher).unwrap();
                    item.id = format!("{hash:016x}-{}", item.id);
                    items.push(item);
                }
            }
            pb.finish();
            items.sort_by(|a, b| a.id.cmp(&b.id));
            let outfile = File::create(&outpath).unwrap();
            let mut writer: Option<Writer<_>> = None;
            for chunk in items.chunks(10000) {
                let items = chunk.to_vec();
                if let Some(writer) = writer.as_mut() {
                    writer.write(items).unwrap();
                } else {
                    writer = Some(WriterBuilder::new(&outfile).build(items).unwrap());
                }
            }
            writer.unwrap().finish().unwrap();
        });
    }
    while let Some(result) = set.join_next().await {
        result.unwrap();
    }
    m.clear().unwrap();
}

fn contains_year(path: &Path, year: &str) -> bool {
    path.to_string()
        .split('_')
        .filter_map(|part| part.split('T').next())
        .filter_map(|date| date.split('-').next())
        .any(|y| y == year)
}

async fn get_sas_token() -> SASToken {
    let body = reqwest::get(format!(
        "https://planetarycomputer.microsoft.com/api/sas/v1/token/{}/{}",
        ACCOUNT, CONTAINER_NAME
    ))
    .await
    .unwrap();
    body.json().await.unwrap()
}

#[derive(Debug, Deserialize)]
struct SASToken {
    #[allow(unused)]
    #[serde(rename = "msft:expiry")]
    expiry: String,
    token: String,
}
