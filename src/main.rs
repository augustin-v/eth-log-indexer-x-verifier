use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::{BlockNumberOrTag, Filter, Log};
use alloy::sol;
use alloy::sol_types::SolEvent;
use clap::{Parser, Subcommand, ValueEnum};
use rusqlite::{Connection, Transaction, params, params_from_iter};
use std::time::Duration;
use tracing::{debug, error, info};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    if let Err(e) = dotenv::dotenv() {
        error!("Failed to load .env file: {}", e);
    }
    let config = Config::from_env();
    let cli = Cli::parse();

    info!(
        "Starting Ethereum Log Indexer with command: {:?}",
        cli.command
    );

    let mut db_conn = Connection::open("transfers.db")?;
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS transfers (
            tx_hash TEXT,
            log_index INTEGER,
            from_addr TEXT,
            to_addr TEXT,
            value TEXT,
            block_number INTEGER,
            block_hash TEXT,
            PRIMARY KEY (tx_hash, log_index)
        )",
        params![],
    )?;
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_block ON transfers (block_number)",
        params![],
    )?;
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_from ON transfers (from_addr)",
        params![],
    )?;
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_to ON transfers (to_addr)",
        params![],
    )?;

    match cli.command {
        Commands::Index {
            finality,
            poll_interval,
            range,
        } => {
            let rpc_url = config.rpc.parse()?;
            let erc20_address: Address = config.erc20.parse()?;

            info!("Connecting to HTTP RPC: {}", config.rpc);
            debug!("ERC-20 address: {}", erc20_address);

            let provider = ProviderBuilder::new().connect_http(rpc_url);

            let chain_id = provider.get_chain_id().await?;
            let latest_block = provider.get_block_number().await?;
            info!("Successfully connected to chain ID: {}", chain_id);
            debug!("Current latest block: {}", latest_block);

            let filter = Filter::new()
                .address(erc20_address)
                .event("Transfer(address,address,uint256)");

            info!("Starting indexing with filter: {:?}", filter);

            let (range_from, range_to) = match range {
                Some(vec) if vec.len() == 2 => (Some(vec[0]), Some(vec[1])),
                Some(_) => {
                    error!("Invalid range: exactly two values required");
                    return Ok(());
                }
                None => (None, None),
            };
            poll_and_index(
                &provider,
                &filter,
                &mut db_conn,
                finality,
                poll_interval,
                range_from,
                range_to,
            )
            .await?;
        }
        Commands::Query { tx_hash, from_addr } => {
            if tx_hash.is_none() && from_addr.is_none() {
                info!("Please provide --tx-hash or --from-addr for querying.");
                return Ok(());
            }

            let mut query = "SELECT * FROM transfers WHERE 1=1".to_string();
            let mut params_vec: Vec<String> = vec![];

            if let Some(hash) = tx_hash {
                query.push_str(" AND tx_hash = ?");
                params_vec.push(hash);
            }
            if let Some(addr) = from_addr {
                query.push_str(" AND from_addr = ?");
                params_vec.push(addr);
            }

            info!("Querying transfers with: {}", query);
            let mut stmt = db_conn.prepare(&query)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut found = false;
            while let Some(row) = rows.next()? {
                found = true;
                let tx_hash: String = row.get(0)?;
                let from: String = row.get(2)?;
                let to: String = row.get(3)?;
                let value: String = row.get(4)?;
                let block: u64 = row.get(5)?;
                info!(
                    "Transfer in tx {}: {} -> {} (value: {}) in block {}",
                    tx_hash, from, to, value, block
                );
            }
            if !found {
                info!("No transfers found matching the query.");
            }
        }
    }

    Ok(())
}

async fn poll_and_index(
    provider: &impl Provider,
    filter: &Filter,
    db_conn: &mut Connection,
    finality_mode: FinalityMode,
    poll_interval_secs: u64,
    range_from: Option<u64>,
    range_to: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_indexed: u64 = get_last_indexed_block(db_conn)?.unwrap_or(0);
    let use_range = range_from.is_some() && range_to.is_some();
    loop {
        let latest = provider.get_block_number().await?;
        let finalized_block = provider
            .get_block(BlockNumberOrTag::Finalized.into())
            .await?
            .ok_or("Failed to get finalized block")?
            .header
            .number;
        let mut end_block = match finality_mode {
            FinalityMode::Safe => {
                provider
                    .get_block(BlockNumberOrTag::Safe.into())
                    .await?
                    .ok_or("Failed to get safe block")?
                    .header
                    .number
            }
            FinalityMode::Finalized => finalized_block,
            FinalityMode::None => latest,
        };

        let mut current = last_indexed + 1;

        if use_range {
            let start = range_from.unwrap();
            let end = range_to.unwrap();
            if start > end || start > latest {
                error!(
                    "Invalid range: start={} > end={} or exceeds latest={}",
                    start, end, latest
                );
                return Ok(());
            }
            current = start;
            end_block = end;
            info!("Indexing specified range: {} to {}", current, end_block);
        }

        if current <= end_block {
            info!("Indexing from block {} to {}", current, end_block);
            while current <= end_block {
                let batch_end = (current + 499).min(end_block);
                let batch_filter = filter.clone().from_block(current).to_block(batch_end);
                let logs = provider.get_logs(&batch_filter).await?;
                let mut tx = db_conn.transaction()?;
                for log in logs {
                    if let Err(e) =
                        process_log(&log, &mut tx, provider, finality_mode.clone()).await
                    {
                        error!("Log processing error: {}", e);
                    }
                }
                tx.commit()?;
                current = batch_end + 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            last_indexed = end_block;
            check_for_reorgs(provider, db_conn, finalized_block).await?;
        } else {
            info!(
                "Up to date at block {}; no new blocks to index",
                last_indexed
            );
        }

        if use_range {
            info!("Range indexing complete; exiting.");
            break;
        }
        info!(
            "Polling complete; sleeping for {} seconds",
            poll_interval_secs
        );
        tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;
    }
    Ok(())
}

async fn process_log(
    log: &Log,
    tx: &mut Transaction<'_>,
    provider: &impl Provider,
    finality_mode: FinalityMode,
) -> Result<(), Box<dyn std::error::Error>> {
    match log.topic0() {
        Some(&Transfer::SIGNATURE_HASH) => {
            match log.log_decode::<Transfer>() {
                Ok(decoded) => {
                    let from = decoded.data().from;
                    let to = decoded.data().to;
                    let value = decoded.data().value;
                    debug!("Decoded: from={}, to={}, value={}", from, to, value);

                    if let (Some(block_num), Some(block_hash), Some(tx_hash), Some(log_index)) = (
                        log.block_number,
                        log.block_hash,
                        log.transaction_hash,
                        log.log_index,
                    ) {
                        // finality check
                        if finality_mode != FinalityMode::None {
                            let tag = match finality_mode {
                                FinalityMode::Safe => BlockNumberOrTag::Safe,
                                FinalityMode::Finalized => BlockNumberOrTag::Finalized,
                                _ => BlockNumberOrTag::Finalized,
                            };
                            let check_block = provider
                                .get_block(tag.into())
                                .await?
                                .ok_or("Failed to fetch block")?;
                            let check_num = check_block.header.number;

                            if block_num > check_num {
                                info!(
                                    "Skipping log in block {} ({:?}: {}) - not finalized yet",
                                    block_num, finality_mode, check_num
                                );
                                return Ok(());
                            }
                        }

                        match tx.execute(
                            "INSERT OR IGNORE INTO transfers (tx_hash, log_index, from_addr, to_addr, value, block_number, block_hash)
                             VALUES (?, ?, ?, ?, ?, ?, ?)",
                            params![
                                tx_hash.to_string(),
                                log_index,
                                from.to_string(),
                                to.to_string(),
                                value.to_string(),
                                block_num,
                                block_hash.to_string()
                            ],
                        ) {
                            Ok(0) => debug!("Log already exists (tx: {}, index: {}) - skipped", tx_hash, log_index),
                            Ok(_) => info!("Stored Transfer (tx: {}): {} -> {} (value: {}) in block {}", tx_hash, from, to, value, block_num),
                            Err(e) => error!("Failed to store transfer: {}", e),
                        }
                    } else {
                        debug!("Log missing required fields, skipping");
                    }
                }
                Err(e) => error!("Failed to decode log: {}\n\n raw log: {:?}", e, log),
            }
        }
        Some(_) => error!("Unexpected event signature in log: {:?}", log),
        None => debug!("Log missing topic0, skipping"),
    }
    Ok(())
}

fn get_last_indexed_block(db_conn: &Connection) -> Result<Option<u64>, rusqlite::Error> {
    let mut stmt = db_conn.prepare("SELECT MAX(block_number) FROM transfers")?;
    stmt.query_row(params![], |row| row.get(0))
}

/// Checks for blockchain reorganizations in recently indexed blocks to ensure data integrity.
/// queries the DB for blocks after (finalized - 100), compares stored hashes with current RPC data,
/// and deletes logs from any mismatched (reorged) blocks to keep the DB consistent with the canonical chain.
async fn check_for_reorgs(
    provider: &impl Provider,
    db_conn: &Connection,
    finalized: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stmt = db_conn.prepare(
        "SELECT DISTINCT block_number, block_hash FROM transfers WHERE block_number > ?",
    )?;
    let rows = stmt.query_map(params![finalized - 100], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    for row in rows {
        let (block_num, stored_hash): (u64, String) = row?;
        if let Some(block) = provider.get_block(block_num.into()).await? {
            if block.header.hash.to_string() != stored_hash {
                info!(
                    "Reorg detected at block {}; deleting affected logs",
                    block_num
                );
                db_conn.execute(
                    "DELETE FROM transfers WHERE block_number = ?",
                    params![block_num],
                )?;
            }
        }
    }
    Ok(())
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Index {
        #[arg(value_enum, default_value = "finalized")]
        finality: FinalityMode,
        #[arg(long, default_value = "10")]
        poll_interval: u64,
        #[arg(long, num_args = 2, value_names = &["FROM", "TO"])]
        range: Option<Vec<u64>>,
    },
    Query {
        #[arg(long)]
        tx_hash: Option<String>,
        #[arg(long)]
        from_addr: Option<String>,
    },
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum FinalityMode {
    None,
    Safe,
    Finalized,
}

#[derive(Clone)]
struct Config {
    erc20: String,
    rpc: String,
}

impl Config {
    fn from_env() -> Self {
        let erc20 = dotenv::var("ERC20_ADDRESS").expect("ERC20_ADDRESS not set");
        let rpc = dotenv::var("RPC").expect("RPC not set");
        Self { erc20, rpc }
    }
}
