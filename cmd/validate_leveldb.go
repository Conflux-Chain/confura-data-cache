package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/cmd"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	validateLeveldbCmdArgs struct {
		url       string
		blockFrom int64
		numBlocks uint64
	}

	validateLeveldbCmd = &cobra.Command{
		Use:   "validate-leveldb",
		Short: "Validate data consistence based on JSON string between fullnode and LevelDB database",
		Run:   validateLeveldb,
	}
)

func init() {
	validateLeveldbCmd.Flags().StringVar(&validateLeveldbCmdArgs.url, "url", "http://evm.confluxrpc.com", "Fullnode RPC endpoint")
	validateLeveldbCmd.Flags().Int64Var(&validateLeveldbCmdArgs.blockFrom, "block-from", -100, "Block number to validate from, negative value means \"finalized\" - N")
	validateLeveldbCmd.Flags().Uint64Var(&validateLeveldbCmdArgs.numBlocks, "blocks", 10, "Number of blocks to validate")

	rootCmd.AddCommand(validateLeveldbCmd)
}

func validateLeveldb(*cobra.Command, []string) {
	client, err := web3go.NewClient(validateLeveldbCmdArgs.url)
	cmd.FatalIfErr(err, "Failed to create client")
	defer client.Close()

	blockFrom, blockTo := mustNormalizeBlockRange(client, validateLeveldbCmdArgs.blockFrom, validateLeveldbCmdArgs.numBlocks, ethTypes.FinalizedBlockNumber)
	logrus.WithField("from", blockFrom).WithField("to", blockTo).Info("Block range normalized")

	path, err := os.MkdirTemp("", "confura-data-cache-")
	cmd.FatalIfErr(err, "Failed to create tmp dir")
	defer os.RemoveAll(path)

	config := leveldb.Config{
		Path:                   path,
		DefaultNextBlockNumber: blockFrom,
	}
	store, err := leveldb.NewStore(config)
	cmd.FatalIfErr(err, "Failed to create store")
	defer store.Close()

	var (
		cache       = make(map[uint64]types.EthBlockData)
		numBlocks   int
		numTxs      int
		numReceipts int
		numTraces   int
	)

	// read data from full node and write into store
	logrus.Info("Begin to retrieve eth block data from fullnode ...")
	for i := blockFrom; i <= blockTo; i++ {
		data, err := types.QueryEthBlockData(client, i)
		cmd.FatalIfErr(err, "Failed to query eth block data")

		err = store.Write(data)
		cmd.FatalIfErr(err, "Failed to write eth block data to store")

		cache[i] = data

		logrus.WithFields(logrus.Fields{
			"block":    i,
			"txs":      len(data.Block.Transactions.Transactions()),
			"receipts": len(data.Receipts),
			"traces":   len(data.Traces),
		}).Debug("Succeeded to retrieve eth block data")

		numBlocks++
		numTxs += len(data.Block.Transactions.Transactions())
		numReceipts += len(data.Receipts)
		numTraces += len(data.Traces)
	}

	// validate data in database
	logrus.WithFields(logrus.Fields{
		"blocks":   numBlocks,
		"txs":      numTxs,
		"receipts": numReceipts,
		"traces":   numTraces,
	}).Info("Begin to validate eth block data in store ...")
	for i := blockFrom; i <= blockTo; i++ {
		mustValidateEthBlockData(cache, store, i)
		logrus.WithFields(logrus.Fields{
			"block":    i,
			"txs":      len(cache[i].Block.Transactions.Transactions()),
			"receipts": len(cache[i].Receipts),
			"traces":   len(cache[i].Traces),
		}).Debug("Succeeded to validate eth block data")
	}

	logrus.WithFields(logrus.Fields{
		"blocks":   numBlocks,
		"txs":      numTxs,
		"receipts": numReceipts,
		"traces":   numTraces,
	}).Info("Succeeded to validate LevelDB")
}

func mustNormalizeBlockRange(client *web3go.Client, blockFrom int64, numBlocks uint64, latestBlockNumberTag ethTypes.BlockNumber) (uint64, uint64) {
	latest, err := client.Eth.BlockByNumber(latestBlockNumberTag, false)
	cmd.FatalIfErr(err, "Failed to get latest block")

	// normalize block from
	var from uint64
	if blockFrom >= 0 {
		from = uint64(blockFrom)
	} else if latest.Number.Int64() < -blockFrom {
		logrus.WithField("latest", latest.Number).Fatal("Invalid block from")
	} else {
		from = uint64(latest.Number.Int64() + blockFrom)
	}

	// check arguments
	if from > latest.Number.Uint64() {
		logrus.WithField("latest", latest.Number).WithField("from", from).Fatal("Invalid block from")
	}

	to := from + numBlocks - 1
	if to > latest.Number.Uint64() {
		logrus.WithField("latest", latest.Number).WithField("to", to).Fatal("Invalid block to")
	}

	return from, to
}

func mustValidateEthBlockData(cache map[uint64]types.EthBlockData, store *leveldb.Store, bn uint64) {
	// block
	block, err := store.GetBlock(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockByNumber", cache[bn].Block, block.MustLoad())
	blockHash := block.MustLoad().Hash

	block, err = store.GetBlock(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockByHash", cache[bn].Block, block.MustLoad())

	txCount, err := store.GetBlockTransactionCount(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockTransactionCountByNumber", len(cache[bn].Block.Transactions.Transactions()), txCount)

	txCount, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockTransactionCountByHash", len(cache[bn].Block.Transactions.Transactions()), txCount)

	// transaction
	for i, v := range cache[bn].Block.Transactions.Transactions() {
		tx, err := store.GetTransactionByHash(v.Hash)
		assertJsonEqual(err, bn, "GetTransactionByHash", v, tx, logrus.Fields{"txIndex": i})

		tx, err = store.GetTransactionByIndex(types.BlockHashOrNumberWithHash(blockHash), uint32(i))
		assertJsonEqual(err, bn, "GetTransactionByBlockHashAndIndex", v, tx, logrus.Fields{"txIndex": i})

		tx, err = store.GetTransactionByIndex(types.BlockHashOrNumberWithNumber(bn), uint32(i))
		assertJsonEqual(err, bn, "GetTransactionByBlockNumberAndIndex", v, tx, logrus.Fields{"txIndex": i})
	}

	// tx receipt
	for i, v := range cache[bn].Receipts {
		receipt, err := store.GetTransactionReceipt(v.TransactionHash)
		assertJsonEqual(err, bn, "GetTransactionReceipt", v, receipt, logrus.Fields{"txIndex": i})
	}

	// block receipts
	receipts, err := store.GetBlockReceipts(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockReceiptsByNumber", cache[bn].Receipts, receipts)

	receipts, err = store.GetBlockReceipts(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockReceiptsByHash", cache[bn].Receipts, receipts)

	// tx traces
	for i, tx := range cache[bn].Block.Transactions.Transactions() {
		var txTraces []ethTypes.LocalizedTrace
		for _, trace := range cache[bn].Traces {
			if *trace.TransactionHash == tx.Hash {
				txTraces = append(txTraces, trace)
			}
		}

		traces, err := store.GetTransactionTraces(tx.Hash)
		assertJsonEqual(err, bn, "GetTransactionTraces", txTraces, traces, logrus.Fields{"txIndex": i})
	}

	// block traces
	traces, err := store.GetBlockTraces(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockTracesByNumber", cache[bn].Traces, traces)

	traces, err = store.GetBlockTraces(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockTracesByHash", cache[bn].Traces, traces)
}

func assertJsonEqual(err error, bn uint64, api string, expected any, actual any, fields ...logrus.Fields) {
	args := logrus.Fields{
		"bn":  bn,
		"api": api,
	}

	if len(fields) > 0 {
		for k, v := range fields[0] {
			args[k] = v
		}
	}

	cmd.FatalIfErr(err, "Failed to get data in store", args)

	expectedJson, err := json.MarshalIndent(expected, "", "    ")
	cmd.FatalIfErr(err, "Failed to JSON marshal expected value")

	actualJson, err := json.MarshalIndent(actual, "", "    ")
	cmd.FatalIfErr(err, "Failed to JSON marshal actual value")

	if crypto.Keccak256Hash(expectedJson) != crypto.Keccak256Hash(actualJson) {
		fmt.Println()
		fmt.Println("================================================================")
		fmt.Println()
		fmt.Println("***** Expect *****")
		fmt.Println(string(expectedJson))
		fmt.Println()
		fmt.Println()
		fmt.Println("***** Actual *****")
		fmt.Println(string(actualJson))
		fmt.Println()
		fmt.Println("================================================================")
		fmt.Println()

		logrus.WithFields(args).Fatal("Data mismatch")
	}
}
