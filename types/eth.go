package types

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
)

type BlockHashOrNumber struct {
	hash   *common.Hash
	number uint64
}

func BlockHashOrNumberWithHex(hex string) BlockHashOrNumber {
	hash := common.HexToHash(hex)
	return BlockHashOrNumberWithHash(hash)
}

func BlockHashOrNumberWithHash(hash common.Hash) BlockHashOrNumber {
	return BlockHashOrNumber{
		hash: &hash,
	}
}

func BlockHashOrNumberWithNumber(blockNumber uint64) BlockHashOrNumber {
	return BlockHashOrNumber{
		number: blockNumber,
	}
}

func (bhon BlockHashOrNumber) HashOrNumber() (common.Hash, bool, uint64) {
	if bhon.hash != nil {
		return *bhon.hash, true, 0
	}

	return common.Hash{}, false, bhon.number
}

func (bhon BlockHashOrNumber) MarshalJSON() ([]byte, error) {
	if bhon.hash != nil {
		return json.Marshal(*bhon.hash)
	}

	return json.Marshal(bhon.number)
}

func (bhon *BlockHashOrNumber) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		bhon.hash = nil
		return json.Unmarshal(data, &bhon.number)
	}

	if bhon.hash == nil {
		bhon.hash = new(common.Hash)
	}

	if err := json.Unmarshal(data, bhon.hash); err != nil {
		return err
	}

	bhon.number = 0

	return nil
}
