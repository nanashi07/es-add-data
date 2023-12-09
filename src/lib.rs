use chrono::Utc;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::Url;
use elasticsearch::{BulkParts, Elasticsearch};
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::fmt::Debug;
use std::str::FromStr;
use uuid::Uuid;

pub type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub async fn init_log(level: &str) -> AppResult<()> {
    let stdout = ConsoleAppender::builder().build();
    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(level).unwrap_or(LevelFilter::Info)),
        )?;

    let _ = log4rs::init_config(config)?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct AnyDocument {
    pub(crate) name: String,
    pub(crate) max: i64,
    pub(crate) min: i64,
    pub(crate) time: String,
}

async fn get_elasticsearch_client(uri: &str) -> AppResult<Elasticsearch> {
    let url = Url::parse(uri)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    Ok(client)
}

pub async fn bulk_index(host: &str, index_name: &str, size: usize) -> AppResult<()> {
    let client: Elasticsearch = get_elasticsearch_client(host).await?;

    let mut body: Vec<JsonBody<_>> = Vec::new();
    for i in 0..size as i64 {
        let document = AnyDocument {
            name: format!("{}", Uuid::new_v4()),
            max: i * 100,
            min: i,
            time: Utc::now().to_rfc3339(),
        };
        log::trace!("{:?}", &document);
        body.push(json!({"index": { "_type": "document" }}).into());
        body.push(json!(document).into());
    }

    let response = client
        .bulk(BulkParts::Index(&index_name))
        .body(body)
        .send()
        .await?;

    log::debug!(
        "response {} for index {}, response: {:?}",
        response.status_code(),
        &index_name,
        response
    );

    Ok(())
}

#[tokio::test]
async fn test_run() -> AppResult<()> {
    init_log("trace").await?;

    let host = "http://192.168.122.191:9200";
    let index = "test_index_01";
    let size = 1000;

    bulk_index(host, index, size).await?;

    Ok(())
}
