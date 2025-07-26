use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::{Filter, Log};
use alloy::sol;
use alloy::sol_types::SolEvent;
use rusqlite::{Connection, params};
use std::error::Error;
use tracing::{debug, error, info};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    if let Err(e) = dotenv::dotenv() {
        error!("Failed to load .env file: {}", e);
    }
    let erc20_address: Address = dotenv::var("ERC20_ADDRESS")?.parse()?;
    let rpc_url = dotenv::var("RPC")?.parse()?;

    info!("Connecting to HTTP RPC: {}", rpc_url);
    debug!("ERC-20 address: {}", erc20_address);

    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let chain_id = provider.get_chain_id().await?;
    info!("Successfully connected to chain ID: {}", chain_id);

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

    let block_num = 23003247; // finalized block
    let filter = Filter::new()
        .address(erc20_address)
        .event("Transfer(address,address,uint256)")
        .from_block(block_num)
        .to_block(block_num);

    info!("Fetching logs for block {}", block_num);
    let logs = provider.get_logs(&filter).await?;

    for log in logs {
        if let Err(e) = process_log(&log, &mut db_conn).await {
            error!("Log processing error: {}", e);
        }
    }

    info!("Indexing complete for block {}", block_num);
    Ok(())
}

async fn process_log(log: &Log, db_conn: &mut Connection) -> Result<(), Box<dyn Error>> {
    match log.topic0() {
        Some(&Transfer::SIGNATURE_HASH) => match log.log_decode::<Transfer>() {
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
                    db_conn.execute(
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
                        )?;
                    info!(
                        "Stored Transfer (tx: {}): {} -> {} (value: {}) in block {}",
                        tx_hash, from, to, value, block_num
                    );
                } else {
                    debug!("Log missing required fields, skipping");
                }
            }
            Err(e) => error!("Failed to decode log: {}\n\n raw log: {:?}", e, log),
        },
        Some(_) => error!("Unexpected event signature in log: {:?}", log),
        None => debug!("Log missing topic0, skipping"),
    }
    Ok(())
}
