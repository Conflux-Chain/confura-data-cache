# confura-data-cache

Confura data cache aims to cache blockchain data in database to reduce RPC node workload. Basically, there are 2 kinds of caches available:

1. [Near head cache](./nearhead): cache the most recent blockchain data in memory.
2. [Database cache](./store/leveldb): cache the finalized blockchain data in database, e.g. LevelDB.

Blockchain data includes ***blocks, transactions, receipts and traces***, which could be replayed from RPC node.

On the contrary, state relevant data, e.g. balance, nonce, still requires to access RPC node.

## Near Head Cache

Generally, most requested blockchain data are between the **finalized** block and the **latest** block, we call it near head data. Such kind of cache could heavily reduce the RPC node workload.

Note, POW chain occasionally forks at the latest block height. Therefore, near head cache must handle the chain reorg correctly.

On the other hand, the cache implementation must guarentee the maximum memory usage to avoid OOM issue.

## LevelDB Database Cache

Once any block is finalized (which means never reorg anymore)，blockchain data could be synchronized from RPC node and persist in offchain LevelDB database. In this way, such data cache could serve most historical data requests.

## Lazy Decode

In case of high QPS, JSON RPC codec may cause high CPU usage. To improve the JSON RPC codec performance, all blockchain data stored in LevelDB database are formatted in JSON. In addition, do not unmarshal JSON data to concrete object (e.g. block, transaction) when load from database. Instead, returns the raw JSON data to client. For more details, please refer to the [Lazy](./types/codec.go) decode mechanism.
