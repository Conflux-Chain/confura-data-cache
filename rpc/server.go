package rpc

import (
	"encoding/hex"
	"net/http"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Endpoint     string   `default:":38545"`
	Cors         []string `default:"[*]"`
	VirtualHosts []string `default:"[*]"`
	JwtSecretHex string   // without 0x prefix
}

func DefaultConfig() (config Config) {
	defaults.SetDefaults(&config)
	return
}

// MustServe starts RPC service until shutdown.
func MustServe(config Config, store *leveldb.Store) error {
	handler := rpc.NewServer()

	if err := handler.RegisterName("eth", NewApi(store)); err != nil {
		logrus.WithError(err).Fatal("Failed to register rpc service")
	}

	jwtSecret, err := hex.DecodeString(config.JwtSecretHex)
	if err != nil {
		logrus.WithError(err).WithField("jwt", config.JwtSecretHex).Fatal("Failed to decode JWT secret in HEX format")
	}

	server := http.Server{
		Addr:    config.Endpoint,
		Handler: node.NewHTTPHandlerStack(handler, config.Cors, config.VirtualHosts, jwtSecret),
	}

	return server.ListenAndServe()
}
