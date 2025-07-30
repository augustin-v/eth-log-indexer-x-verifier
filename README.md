# Quick Start
This Rust tool indexes ERC-20 Transfer events from Ethereum blocks and stores them in a local SQLite database (`transfers.db`). It supports continuous polling, range-based indexing, reorg handling, and querying stored data.

To use it:
- copy paste `.env.example` ->  `.env` (you can custom it or not, the default address monitored is the USDC on Ethereum)
- build with :
```bash
cargo build -r
```

To run it to index Transfer events from a specific block range :
```bash
 ./target/release/eth-log-indexer index --range <start_block> <end_block>
 ```
 To query Transfer events transaction hashes that were stored in the db, run:
```bash
./target/release/eth-log-indexer query --tx-hash <tx_hash>
```

To see all of the options, run 
```bash
./target/release/eth-log-indexer query --help # for querying the db
./target/release/eth-log-indexer index --help # for indexing
```

--- 

The tool uses a local SQLite database (`transfers.db`) with the following schema
```
CREATE TABLE IF NOT EXISTS transfers (
tx_hash TEXT,
log_index INTEGER,
from_addr TEXT,
to_addr TEXT,
value TEXT,
block_number INTEGER,
block_hash TEXT,
PRIMARY KEY (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_block ON transfers (block_number);
CREATE INDEX IF NOT EXISTS idx_from ON transfers (from_addr);
CREATE INDEX IF NOT EXISTS idx_to ON transfers (to_addr);
````
