package rpc

import (
	"net/http"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

// MustServe starts RPC service until shutdown.
func MustServe(endpoint string, store *leveldb.Store) error {
	handler := rpc.NewServer()

	if err := handler.RegisterName("eth", NewApi(store)); err != nil {
		logrus.WithError(err).Fatal("Failed to register rpc service")
	}

	server := http.Server{
		Addr:    endpoint,
		Handler: node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"}, []byte{}),
	}

	return server.ListenAndServe()
}
