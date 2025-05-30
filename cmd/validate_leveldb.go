package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/rpc"
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

	// normalize block range
	blockFrom, blockTo := mustNormalizeBlockRange(client, validateLeveldbCmdArgs.blockFrom, validateLeveldbCmdArgs.numBlocks, ethTypes.FinalizedBlockNumber)

	// prepare tmp db
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

	// start rpc and gRPC service
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	rpcConfig := rpc.DefaultConfig()
	go rpc.MustServeRPC(ctx, &wg, rpcConfig, store)
	go rpc.MustServeGRPC(ctx, &wg, rpcConfig, store)
	time.Sleep(time.Second)

	// prepare rpc and rRPC client
	rpcClient, err := rpc.NewClient(fmt.Sprintf("http://127.0.0.1%v", rpcConfig.Endpoint))
	cmd.FatalIfErr(err, "Failed to create rpc client")
	defer rpcClient.Close()
	grpcClient, err := rpc.NewClientProto(fmt.Sprintf("127.0.0.1%v", rpcConfig.Proto.Endpoint))
	cmd.FatalIfErr(err, "Failed to create grpc client")

	var (
		cache = make(map[uint64]EthBlockDataExt)
		stat  struct {
			Blocks   int
			Txs      int
			Receipts int
			Traces   int
		}
	)

	// read data from full node and write into store
	logrus.WithField("from", blockFrom).WithField("to", blockTo).Info("Begin to retrieve eth block data from fullnode ...")
	for i := blockFrom; i <= blockTo; i++ {
		var data EthBlockDataExt

		data.EthBlockData, err = types.QueryEthBlockData(client, i)
		cmd.FatalIfErr(err, "Failed to query eth block data")

		data.BlockSummary, err = client.Eth.BlockByNumber(ethTypes.NewBlockNumber(int64(i)), false)
		cmd.FatalIfErr(err, "Failed to query block without txs")

		err = store.Write(data.EthBlockData)
		cmd.FatalIfErr(err, "Failed to write eth block data to store")

		cache[i] = data

		logrus.WithFields(logrus.Fields{
			"block":    i,
			"txs":      len(data.Block.Transactions.Transactions()),
			"receipts": len(data.Receipts),
			"traces":   len(data.Traces),
		}).Debug("Succeeded to retrieve eth block data")

		stat.Blocks++
		stat.Txs += len(data.Block.Transactions.Transactions())
		stat.Receipts += len(data.Receipts)
		stat.Traces += len(data.Traces)
	}

	// validate data against rpc and gRPC
	logrus.WithField("stat", fmt.Sprintf("%+v", stat)).Info("Begin to validate eth block data ...")
	for i := blockFrom; i <= blockTo; i++ {
		mustValidateEthBlockData(cache, rpcClient, i)
		mustValidateEthBlockData(cache, grpcClient, i)

		logrus.WithFields(logrus.Fields{
			"block":    i,
			"txs":      len(cache[i].Block.Transactions.Transactions()),
			"receipts": len(cache[i].Receipts),
			"traces":   len(cache[i].Traces),
		}).Debug("Succeeded to validate eth block data")
	}

	logrus.WithField("stat", fmt.Sprintf("%+v", stat)).Info("Succeeded to validate LevelDB")

	cancel()
	wg.Wait()
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

func mustValidateEthBlockData(cache map[uint64]EthBlockDataExt, client rpc.Interface, bn uint64) {
	// block - by number
	block, err := client.GetBlock(types.BlockHashOrNumberWithNumber(bn), true)
	assertJsonEqual(err, bn, "GetBlockByNumber_true", cache[bn].Block, block.MustLoad())

	block, err = client.GetBlock(types.BlockHashOrNumberWithNumber(bn), false)
	assertJsonEqual(err, bn, "GetBlockByNumber_false", cache[bn].BlockSummary, block.MustLoad())

	// block - by hash
	blockHash := block.MustLoad().Hash
	block, err = client.GetBlock(types.BlockHashOrNumberWithHash(blockHash), true)
	assertJsonEqual(err, bn, "GetBlockByHash_true", cache[bn].Block, block.MustLoad())

	block, err = client.GetBlock(types.BlockHashOrNumberWithHash(blockHash), false)
	assertJsonEqual(err, bn, "GetBlockByHash_false", cache[bn].BlockSummary, block.MustLoad())

	// block txs - by number
	txCount, err := client.GetBlockTransactionCount(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockTransactionCountByNumber", len(cache[bn].Block.Transactions.Transactions()), txCount)

	// block txs - by hash
	txCount, err = client.GetBlockTransactionCount(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockTransactionCountByHash", len(cache[bn].Block.Transactions.Transactions()), txCount)

	// transaction
	for i, v := range cache[bn].Block.Transactions.Transactions() {
		tx, err := client.GetTransactionByHash(v.Hash)
		assertJsonEqual(err, bn, "GetTransactionByHash", v, tx, logrus.Fields{"txIndex": i})

		tx, err = client.GetTransactionByIndex(types.BlockHashOrNumberWithHash(blockHash), uint32(i))
		assertJsonEqual(err, bn, "GetTransactionByBlockHashAndIndex", v, tx, logrus.Fields{"txIndex": i})

		tx, err = client.GetTransactionByIndex(types.BlockHashOrNumberWithNumber(bn), uint32(i))
		assertJsonEqual(err, bn, "GetTransactionByBlockNumberAndIndex", v, tx, logrus.Fields{"txIndex": i})
	}

	// tx receipt
	for i, v := range cache[bn].Receipts {
		receipt, err := client.GetTransactionReceipt(v.TransactionHash)
		assertJsonEqual(err, bn, "GetTransactionReceipt", v, receipt, logrus.Fields{"txIndex": i})
	}

	// block receipts
	receipts, err := client.GetBlockReceipts(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockReceiptsByNumber", cache[bn].Receipts, receipts)

	receipts, err = client.GetBlockReceipts(types.BlockHashOrNumberWithHash(blockHash))
	assertJsonEqual(err, bn, "GetBlockReceiptsByHash", cache[bn].Receipts, receipts)

	// tx traces
	for i, tx := range cache[bn].Block.Transactions.Transactions() {
		var txTraces []ethTypes.LocalizedTrace
		for _, trace := range cache[bn].Traces {
			if *trace.TransactionHash == tx.Hash {
				txTraces = append(txTraces, trace)
			}
		}

		traces, err := client.GetTransactionTraces(tx.Hash)
		assertJsonEqual(err, bn, "GetTransactionTraces", txTraces, traces, logrus.Fields{"txIndex": i})
	}

	// block traces
	traces, err := client.GetBlockTraces(types.BlockHashOrNumberWithNumber(bn))
	assertJsonEqual(err, bn, "GetBlockTracesByNumber", cache[bn].Traces, traces)

	traces, err = client.GetBlockTraces(types.BlockHashOrNumberWithHash(blockHash))
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

type EthBlockDataExt struct {
	types.EthBlockData
	BlockSummary *ethTypes.Block // block with only tx hashes
}
