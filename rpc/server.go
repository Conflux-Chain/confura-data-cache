package rpc

import (
	"context"
	"encoding/hex"
	"net"
	"net/http"
	"sync"

	pb "github.com/Conflux-Chain/confura-data-cache/rpc/proto"
	"github.com/Conflux-Chain/confura-data-cache/store"
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

	LruCacheSize int `default:"4096"`

	Proto struct {
		Endpoint       string `default:":48545"`
		MaxMessageSize int    `default:"67108864"` // 64MB
	}
}

func DefaultConfig() (config Config) {
	defaults.SetDefaults(&config)
	return
}

// MustServeRPC starts RPC service and wait for graceful shutdown.
func MustServeRPC(ctx context.Context, wg *sync.WaitGroup, config Config, store store.Store) {
	defer wg.Done()

	listener, err := net.Listen("tcp", config.Endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", config.Endpoint).Fatal("Failed to listen for RPC service")
	}

	handler := rpc.NewServer()

	if err := handler.RegisterName("eth", NewApi(store, config.LruCacheSize)); err != nil {
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

	logrus.WithField("endpoint", config.Endpoint).Info("Succeeded to start RPC service")

	<-ctx.Done()

	server.Close()
}

// MustServeGRPC starts gRPC service and wait for graceful shutdown.
func MustServeGRPC(ctx context.Context, wg *sync.WaitGroup, config Config, store store.Store) {
	defer wg.Done()

	listener, err := net.Listen("tcp", config.Proto.Endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", config.Proto.Endpoint).Fatal("Failed to listen for gRPC service")
	}

	server := grpc.NewServer(grpc.MaxRecvMsgSize(config.Proto.MaxMessageSize))
	pb.RegisterEthServer(server, NewApiProto(NewApi(store, config.LruCacheSize)))
	go server.Serve(listener)

	logrus.WithField("endpoint", config.Proto.Endpoint).Info("Succeeded to run gRPC service")

	<-ctx.Done()

	server.GracefulStop()
}
