package nearhead

import (
	"encoding/json"
	"math/big"
	"testing"
	"unsafe"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/DmitriyVTitov/size"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

var (
	bnNotCached   = uint64(10101010)
	hashNotCached = common.HexToHash("0x1010101010101010101010101010101010101010101010101010101010101010")
)

func createTestCache() *EthCache {
	var cfg Config
	viper.MustUnmarshalKey("nearHead", &cfg)
	cache := NewEthCache(cfg)
	return cache
}

func createTestData(t *testing.T) *types.EthBlockData {
	blockJson := "{\"author\":\"0x1247a0ff7a51b3613aacf292c43f8a530f2dbfdf\",\"baseFeePerGas\":\"0x4a817c800\",\"difficulty\":\"0x179abb20a34\",\"extraData\":\"0x\",\"gasLimit\":\"0x1c9c380\",\"gasUsed\":\"0x1cfc8\",\"hash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"logsBloom\":\"0x00000000000000000000000000000000000200000000000000000000000000000040020000080000100000000000000000000000000000000008000000200000000000000000001000000028000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000400010000000000000000001000000000000000000000000000004000000000000000000000000020000000000000000000000000000000000100000000000000080000000000000000002000000000000000000000000000000000000000000010000001000000010000000000000000000000000000000000000000000020000000000000000\",\"miner\":\"0x1247a0ff7a51b3613aacf292c43f8a530f2dbfdf\",\"mixHash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"nonce\":\"0x77505751b096d294\",\"number\":\"0x729c393\",\"parentHash\":\"0x275eb9c8b9c077a4a78a487a2aba08e1d58247d578b668d8558597a8025ff50d\",\"receiptsRoot\":\"0x12af19d53c378426ebe08ad33e48caf3efdaaade0994770c161c0637e65a6566\",\"size\":\"0x11c\",\"stateRoot\":\"0xd57cdf6422c8a9964be8b017adb88ef2288f3e430915038bc5d0c0c91ccf7240\",\"timestamp\":\"0x67f5012a\",\"totalDifficulty\":\"0x0\",\"transactions\":[{\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"blockNumber\":\"0x729c393\",\"chainId\":\"0x406\",\"from\":\"0x2d26b1202078e49d036d59451f0da60f645e6df6\",\"gas\":\"0x213e6\",\"gasPrice\":\"0x4a817c800\",\"hash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"input\":\"0xc8173c440101984d4c90c00000000000a1d3ec8d000034433d0067f515ec02ca2201f7229fc0abe258d77989205926662e0fd3d2f8f6a4ed97fe170c4d4888c3f0126de4c53627e4a26c0f0d0cd4e4e2b6c3be9c128a11a94ea6b3458529ea8cf09e52e00000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6000000000000000000000000d9f07924cad1298c6a0f6e510122d5f05074bb4c\",\"maxFeePerGas\":\"0x4a817c800\",\"maxPriorityFeePerGas\":\"0x4a817c800\",\"nonce\":\"0xdf\",\"publicKey\":\"0xee4c346da7b8e0660b8d3ee863d7a13b2c105d9f691cba97c4aab137b1a13a790b886b32be24582ca1f1820df83aef0680523bacd6b763937d531b4058567af8\",\"r\":\"0xae2ed9f10150982b567b054d365c6c56c238d7c619379fbc885274427e6d9060\",\"raw\":\"0x02f9011582040681df8504a817c8008504a817c800830213e69425ab3efd52e6470681ce037cd546dc60726948d380b8a4c8173c440101984d4c90c00000000000a1d3ec8d000034433d0067f515ec02ca2201f7229fc0abe258d77989205926662e0fd3d2f8f6a4ed97fe170c4d4888c3f0126de4c53627e4a26c0f0d0cd4e4e2b6c3be9c128a11a94ea6b3458529ea8cf09e52e00000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6000000000000000000000000d9f07924cad1298c6a0f6e510122d5f05074bb4cc001a0ae2ed9f10150982b567b054d365c6c56c238d7c619379fbc885274427e6d9060a025922e9f73080970c8bf8f5b93513bbcc3c7cadd45471363fd1129e4d4bda2f5\",\"s\":\"0x25922e9f73080970c8bf8f5b93513bbcc3c7cadd45471363fd1129e4d4bda2f5\",\"standardV\":\"0x1\",\"status\":\"0x1\",\"to\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"transactionIndex\":\"0x0\",\"type\":\"0x2\",\"v\":\"0x1\",\"value\":\"0x0\",\"yParity\":\"0x1\"}],\"transactionsRoot\":\"0xe9d3b50689d0b0e0f5cef996cc21b1b0cf0013143e76ce00d8863f890f5a8cba\",\"uncles\":[],\"sha3Uncles\":\"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347\"}"
	block := ethTypes.Block{}
	err := json.Unmarshal([]byte(blockJson), &block)
	assert.Nil(t, err)

	receiptsJson := "[{\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"blockNumber\":\"0x729c393\",\"burntGasFee\":\"0x437d32aa62000\",\"contractAddress\":null,\"cumulativeGasUsed\":\"0x1cfc8\",\"effectiveGasPrice\":\"0x4a817c800\",\"from\":\"0x2d26b1202078e49d036d59451f0da60f645e6df6\",\"gasUsed\":\"0x1cfc8\",\"logs\":[{\"address\":\"0xfe97e85d13abd9c1c33384e796f10b73905637ce\",\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"blockNumber\":\"0x729c393\",\"data\":\"0x0000000000000000000000000000000000000000000000001965774de2f70000\",\"logIndex\":\"0x0\",\"removed\":false,\"topics\":[\"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925\",\"0x0000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6\",\"0x00000000000000000000000025ab3efd52e6470681ce037cd546dc60726948d3\"],\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"transactionIndex\":\"0x0\",\"transactionLogIndex\":\"0x0\"},{\"address\":\"0xfe97e85d13abd9c1c33384e796f10b73905637ce\",\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"blockNumber\":\"0x729c393\",\"data\":\"0x0000000000000000000000000000000000000000000001735934a93e6d990000\",\"logIndex\":\"0x1\",\"removed\":false,\"topics\":[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x0000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6\",\"0x00000000000000000000000025ab3efd52e6470681ce037cd546dc60726948d3\"],\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"transactionIndex\":\"0x0\",\"transactionLogIndex\":\"0x1\"},{\"address\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"blockNumber\":\"0x729c393\",\"data\":\"0x\",\"logIndex\":\"0x2\",\"removed\":false,\"topics\":[\"0x8d92c805c252261fcfff21ee60740eb8a38922469a7e6ee396976d57c22fc1c9\",\"0x0101984d4c90c00000000000a1d3ec8d000034433d0067f515ec02ca2201f722\"],\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"transactionIndex\":\"0x0\",\"transactionLogIndex\":\"0x2\"}],\"logsBloom\":\"0x00000000000000000000000000000000000200000000000000000000000000000040020000080000100000000000000000000000000000000008000000200000000000000000001000000028000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000400010000000000000000001000000000000000000000000000004000000000000000000000000020000000000000000000000000000000000100000000000000080000000000000000002000000000000000000000000000000000000000000010000001000000010000000000000000000000000000000000000000000020000000000000000\",\"status\":\"0x1\",\"to\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"transactionIndex\":\"0x0\",\"type\":\"0x2\"}]"
	rcpts := make([]ethTypes.Receipt, 0)
	err = json.Unmarshal([]byte(receiptsJson), &rcpts)
	assert.Nil(t, err)

	tracesJson := "[{\"type\":\"call\",\"action\":{\"from\":\"0x2d26b1202078e49d036d59451f0da60f645e6df6\",\"to\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"value\":\"0x0\",\"gas\":\"0x19e4e\",\"input\":\"0xc8173c440101984d4c90c00000000000a1d3ec8d000034433d0067f515ec02ca2201f7229fc0abe258d77989205926662e0fd3d2f8f6a4ed97fe170c4d4888c3f0126de4c53627e4a26c0f0d0cd4e4e2b6c3be9c128a11a94ea6b3458529ea8cf09e52e00000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6000000000000000000000000d9f07924cad1298c6a0f6e510122d5f05074bb4c\",\"callType\":\"call\"},\"result\":{\"gasUsed\":\"0x441e\",\"output\":\"0x\"},\"traceAddress\":[],\"subtraces\":1,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true},{\"type\":\"call\",\"action\":{\"from\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"to\":\"0x6bb2195a38d8d7ec9d30cb77557eb09a363beacf\",\"value\":\"0x0\",\"gas\":\"0x1916d\",\"input\":\"0xc8173c440101984d4c90c00000000000a1d3ec8d000034433d0067f515ec02ca2201f7229fc0abe258d77989205926662e0fd3d2f8f6a4ed97fe170c4d4888c3f0126de4c53627e4a26c0f0d0cd4e4e2b6c3be9c128a11a94ea6b3458529ea8cf09e52e00000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6000000000000000000000000d9f07924cad1298c6a0f6e510122d5f05074bb4c\",\"callType\":\"delegatecall\"},\"result\":{\"gasUsed\":\"0x3de5\",\"output\":\"0x\"},\"traceAddress\":[],\"subtraces\":2,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true},{\"type\":\"call\",\"action\":{\"from\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"to\":\"0x0000000000000000000000000000000000000001\",\"value\":\"0x0\",\"gas\":\"0x17bc3\",\"input\":\"0x51835b80205bce876de1c2920cb8c8ba12c1679b4f9f75a0b209605ca2bc201f000000000000000000000000000000000000000000000000000000000000001c9fc0abe258d77989205926662e0fd3d2f8f6a4ed97fe170c4d4888c3f0126de4453627e4a26c0f0d0cd4e4e2b6c3be9c128a11a94ea6b3458529ea8cf09e52e0\",\"callType\":\"staticcall\"},\"result\":{\"gasUsed\":\"0x1700b\",\"output\":\"0x0000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df6\"},\"traceAddress\":[],\"subtraces\":0,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true},{\"type\":\"call\",\"action\":{\"from\":\"0x25ab3efd52e6470681ce037cd546dc60726948d3\",\"to\":\"0xfe97e85d13abd9c1c33384e796f10b73905637ce\",\"value\":\"0x0\",\"gas\":\"0xafa5\",\"input\":\"0x23b872dd0000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df600000000000000000000000025ab3efd52e6470681ce037cd546dc60726948d30000000000000000000000000000000000000000000001735934a93e6d990000\",\"callType\":\"call\"},\"result\":{\"gasUsed\":\"0x41cb\",\"output\":\"0x0000000000000000000000000000000000000000000000000000000000000001\"},\"traceAddress\":[],\"subtraces\":2,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true},{\"type\":\"call\",\"action\":{\"from\":\"0xfe97e85d13abd9c1c33384e796f10b73905637ce\",\"to\":\"0x50bc460bfd2f13ff079298a094b00c457c33cef4\",\"value\":\"0x0\",\"gas\":\"0xa399\",\"input\":\"0x5c60da1b\",\"callType\":\"staticcall\"},\"result\":{\"gasUsed\":\"0x9fa9\",\"output\":\"0x0000000000000000000000000a90de1dcc7f715eccca88d0417138ce4c274813\"},\"traceAddress\":[],\"subtraces\":0,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true},{\"type\":\"call\",\"action\":{\"from\":\"0xfe97e85d13abd9c1c33384e796f10b73905637ce\",\"to\":\"0x0a90de1dcc7f715eccca88d0417138ce4c274813\",\"value\":\"0x0\",\"gas\":\"0x9bf9\",\"input\":\"0x23b872dd0000000000000000000000002d26b1202078e49d036d59451f0da60f645e6df600000000000000000000000025ab3efd52e6470681ce037cd546dc60726948d30000000000000000000000000000000000000000000001735934a93e6d990000\",\"callType\":\"delegatecall\"},\"result\":{\"gasUsed\":\"0x3f7b\",\"output\":\"0x0000000000000000000000000000000000000000000000000000000000000001\"},\"traceAddress\":[],\"subtraces\":0,\"transactionPosition\":0,\"transactionHash\":\"0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19\",\"blockNumber\":120177555,\"blockHash\":\"0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6\",\"valid\":true}]"
	traces := make([]ethTypes.LocalizedTrace, 0)
	err = json.Unmarshal([]byte(tracesJson), &traces)
	assert.Nil(t, err)

	return &types.EthBlockData{
		Block:    &block,
		Receipts: rcpts,
		Traces:   traces,
	}
}

func createTestDataBatch(t *testing.T, size int) []types.EthBlockData {
	datas := make([]types.EthBlockData, 0, size)
	for i := 0; i < size; i++ {
		data := createTestData(t)
		data.Block.Number = incrNumber(data.Block.Number, int64(i))
		data.Block.Hash = incrHash(data.Block.Hash, int64(i))
		for _, tx := range data.Block.Transactions.Transactions() {
			tx.Hash = incrHash(tx.Hash, int64(i))
		}
		datas = append(datas, *data)
	}
	return datas
}

func incrNumber(number *big.Int, val ...int64) *big.Int {
	var delta *big.Int
	if len(val) > 0 {
		delta = big.NewInt(val[0])
	} else {
		delta = big.NewInt(1)
	}

	sum := new(big.Int)
	sum.Add(number, delta)

	return sum
}

func incrHash(hash common.Hash, val ...int64) common.Hash {
	var delta *big.Int
	if len(val) > 0 {
		delta = big.NewInt(val[0])
	} else {
		delta = big.NewInt(1)
	}

	sum := new(big.Int)
	sum.Add(hash.Big(), delta)

	return common.BigToHash(sum)
}

func TestEthCache_Put(t *testing.T) {
	// add one block
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))
	assert.Equal(t, cache.start, uint64(120177555))
	assert.Equal(t, cache.end, uint64(120177556))
	assert.Equal(t, cache.currentSize, data.Size())

	// not cached in sequence
	data2 := createTestData(t)
	data2.Block.Number = incrNumber(data2.Block.Number, int64(2))
	data2.Block.Hash = incrHash(data2.Block.Hash, int64(2))
	for _, tx := range data2.Block.Transactions.Transactions() {
		tx.Hash = incrHash(tx.Hash, int64(2))
	}
	assert.ErrorContains(t, cache.Put(data), "Block data not cached in sequence")

	// pop one block
	cache.evict()
	assert.Equal(t, cache.start, uint64(120177555))
	assert.Equal(t, cache.end, uint64(120177556))
	assert.Equal(t, cache.currentSize, data.Size())

	// add multi blocks
	cache = createTestCache()
	batchBlocks := 100
	datas := createTestDataBatch(t, batchBlocks)
	for _, data := range datas {
		assert.Nil(t, cache.Put(&data))
	}
	assert.Equal(t, cache.end, uint64(120177555+batchBlocks))
	assert.Greater(t, cache.currentSize, uint64(0))
	assert.Less(t, cache.currentSize, cache.config.MaxMemory)

	// pop multi block
	for i := 0; i < batchBlocks; i++ {
		cache.evict()
	}
	assert.Equal(t, cache.start, uint64(120177555+batchBlocks-1))
	assert.Equal(t, cache.end, uint64(120177555+batchBlocks))
	assert.Greater(t, cache.currentSize, uint64(0))
}

func TestEthCache_GetBlockByHash(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Len(t, data.Block.Transactions.Transactions(), 1)
	assert.Nil(t, cache.Put(data))

	// get block by hash with tx details
	block := cache.GetBlockByHash(common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), false)
	assert.NotNil(t, block)
	assert.Equal(t, uint64(120177555), block.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), block.Hash)
	assert.Len(t, block.Transactions.Hashes(), 1)

	// get block by hash with tx hashes
	block = cache.GetBlockByHash(common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), true)
	assert.NotNil(t, block)
	assert.Equal(t, uint64(120177555), block.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), block.Hash)
	assert.Len(t, block.Transactions.Transactions(), 1)

	// get block by hash that not exists
	block = cache.GetBlockByHash(hashNotCached, false)
	assert.Nil(t, block)
}

func TestEthCache_GetBlockByNumber(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	// get block by number with tx details
	block := cache.GetBlockByNumber(120177555, false)
	assert.NotNil(t, block)
	assert.Equal(t, uint64(120177555), block.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), block.Hash)
	assert.Len(t, block.Transactions.Hashes(), 1)

	// get block by number with tx hashes
	block = cache.GetBlockByNumber(120177555, true)
	assert.NotNil(t, block)
	assert.Equal(t, uint64(120177555), block.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), block.Hash)
	assert.Len(t, block.Transactions.Transactions(), 1)

	// get block by number that not exists
	block = cache.GetBlockByNumber(bnNotCached, false)
	assert.Nil(t, block)
}

func TestEthCache_GetTransactionByHash(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	tx := cache.GetTransactionByHash(common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"))
	assert.NotNil(t, tx)
	assert.Equal(t, uint64(120177555), tx.BlockNumber.Uint64())
	assert.Equal(t, common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"), tx.Hash)

	// get tx by hash that not exists
	tx = cache.GetTransactionByHash(hashNotCached)
	assert.Nil(t, tx)
}

func TestEthCache_GetBlockReceipts(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	// get block receipts by number
	receipts := cache.GetBlockReceiptsByNumber(120177555)
	assert.NotNil(t, receipts)
	assert.Len(t, receipts, 1)
	assert.Equal(t, uint64(120177555), receipts[0].BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), receipts[0].BlockHash)
	assert.Equal(t, common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"), receipts[0].TransactionHash)

	// get block receipts by hash
	receipts = cache.GetBlockReceiptsByHash(common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"))
	assert.NotNil(t, receipts)
	assert.Len(t, receipts, 1)
	assert.Equal(t, uint64(120177555), receipts[0].BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), receipts[0].BlockHash)
	assert.Equal(t, common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"), receipts[0].TransactionHash)

	// get block receipts by number that not exists
	receipts = cache.GetBlockReceiptsByNumber(bnNotCached)
	assert.Nil(t, receipts)

	// get block receipts by hash that not exists
	receipts = cache.GetBlockReceiptsByHash(hashNotCached)
	assert.Nil(t, receipts)
}

func TestEthCache_GetTransactionReceipt(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	receipt := cache.GetTransactionReceipt(common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"))
	assert.NotNil(t, receipt)
	assert.Equal(t, uint64(120177555), receipt.BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), receipt.BlockHash)
	assert.Equal(t, common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"), receipt.TransactionHash)

	// get tx receipt by hash that not exists
	receipt = cache.GetTransactionReceipt(hashNotCached)
	assert.Nil(t, receipt)
}

func TestEthCache_GetBlockTraces(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	// get block traces by number
	traces := cache.GetBlockTracesByNumber(120177555)
	assert.NotNil(t, traces)
	assert.Len(t, traces, 6)
	assert.Equal(t, uint64(120177555), traces[0].BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), traces[0].BlockHash)

	// get block traces by hash
	traces = cache.GetBlockTracesByHash(common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"))
	assert.NotNil(t, traces)
	assert.Len(t, traces, 6)
	assert.Equal(t, uint64(120177555), traces[0].BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), traces[0].BlockHash)

	// get block traces by number that not exists
	traces = cache.GetBlockTracesByNumber(bnNotCached)
	assert.Nil(t, traces)

	// get block traces by hash that not exists
	traces = cache.GetBlockTracesByHash(hashNotCached)
	assert.Nil(t, traces)
}

func TestEthCache_GetTransactionTraces(t *testing.T) {
	cache := createTestCache()
	data := createTestData(t)
	assert.Nil(t, cache.Put(data))

	traces := cache.GetTransactionTraces(common.HexToHash("0x302df74adbc6f7481d341c2e09814b7e777624d05e3caccbc51a351f7749bb19"))
	assert.NotNil(t, traces)
	assert.Len(t, traces, 6)
	assert.Equal(t, uint64(120177555), traces[0].BlockNumber)
	assert.Equal(t, common.HexToHash("0x5f9cecca56bd3bfda5ba448b36e7f22c9448ed52b2eff79379e38ab5b4c421e6"), traces[0].BlockHash)

	// get tx traces by hash that not exists
	traces = cache.GetTransactionTraces(hashNotCached)
	assert.Nil(t, traces)
}

func TestEthCache_Sizeof(t *testing.T) {
	data := createTestData(t)
	block := data.Block
	estimate := len(block.Author.Bytes())
	estimate += len(block.BaseFeePerGas.Bytes())
	estimate += len(block.Difficulty.Bytes())
	estimate += len(block.ExtraData)
	estimate += int(unsafe.Sizeof(block.GasLimit))
	estimate += int(unsafe.Sizeof(block.GasUsed))
	estimate += len(block.Hash.Bytes())
	estimate += len(block.LogsBloom.Bytes())
	estimate += len(block.Miner.Bytes())
	estimate += len(block.MixHash.Bytes())
	estimate += int(unsafe.Sizeof(block.Nonce.Uint64()))
	estimate += len(block.Number.Bytes())
	estimate += len(block.ParentHash.Bytes())
	estimate += len(block.ReceiptsRoot.Bytes())
	estimate += int(unsafe.Sizeof(block.Size))
	estimate += len(block.StateRoot.Bytes())
	estimate += int(unsafe.Sizeof(block.Timestamp))
	estimate += len(block.TotalDifficulty.Bytes())
	estimate += len(block.TransactionsRoot.Bytes())
	estimate += len(block.Sha3Uncles.Bytes())
	for _, tx := range block.Transactions.Transactions() {
		estimate += len(tx.BlockHash.Bytes())
		estimate += len(tx.BlockNumber.Bytes())
		estimate += len(tx.ChainID.Bytes())
		estimate += len(tx.From.Bytes())
		estimate += int(unsafe.Sizeof(tx.Gas))
		estimate += len(tx.GasPrice.Bytes())
		estimate += len(tx.Hash.Bytes())
		estimate += len(tx.Input)
		estimate += len(tx.MaxFeePerGas.Bytes())
		estimate += len(tx.MaxPriorityFeePerGas.Bytes())
		estimate += int(unsafe.Sizeof(tx.Nonce))
		estimate += len(*tx.PublicKey)
		estimate += len(tx.R.Bytes())
		estimate += len(*tx.Raw)
		estimate += len(tx.S.Bytes())
		estimate += len(tx.StandardV.Bytes())
		estimate += int(unsafe.Sizeof(*tx.Status))
		estimate += len(tx.To.Bytes())
		estimate += int(unsafe.Sizeof(*tx.TransactionIndex))
		estimate += int(unsafe.Sizeof(*tx.Type))
		estimate += len(tx.V.Bytes())
		estimate += len(tx.Value.Bytes())
		estimate += int(unsafe.Sizeof(*tx.YParity))
	}
	blockSize := size.Of(block)
	assert.Greater(t, blockSize, estimate)
	assert.Less(t, blockSize, 3000)
}
