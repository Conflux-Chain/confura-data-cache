package rpc

import (
	"context"
	"encoding/hex"
	"net"
	"net/http"
	"sync"

	pb "github.com/Conflux-Chain/confura-data-cache/rpc/proto"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Config struct {
	Endpoint     string   `default:":38545"`
	Cors         []string `default:"[*]"`
	VirtualHosts []string `default:"[*]"`
	JwtSecretHex string   // without 0x prefix

	Proto struct {
		Endpoint string `default:":48545"`
	}
}

func DefaultConfig() (config Config) {
	defaults.SetDefaults(&config)
	return
}

// MustStartRPC starts RPC service.
func MustStartRPC(ctx context.Context, wg *sync.WaitGroup, config Config, store *leveldb.Store) {
	wg.Add(1)

	listener, err := net.Listen("tcp", config.Endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", config.Endpoint).Fatal("Failed to listen for RPC service")
	}

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

	go server.Serve(listener)

	logrus.WithField("endpoint", config.Endpoint).Info("Succeeded to run RPC service")

	go func() {
		defer wg.Done()
		<-ctx.Done()
		server.Close()
	}()
}

// MustStartGRPC starts gRPC service.
func MustStartGRPC(ctx context.Context, wg *sync.WaitGroup, config Config, store *leveldb.Store) {
	wg.Add(1)

	listener, err := net.Listen("tcp", config.Proto.Endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", config.Proto.Endpoint).Fatal("Failed to listen for gRPC service")
	}

	server := grpc.NewServer()
	pb.RegisterEthServer(server, NewApiProto(NewApi(store)))
	go server.Serve(listener)

	logrus.WithField("endpoint", config.Proto.Endpoint).Info("Succeeded to run gRPC service")

	go func() {
		defer wg.Done()
		<-ctx.Done()
		server.GracefulStop()
	}()
}
