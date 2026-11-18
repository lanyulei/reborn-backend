// Package main 是容器平台的 Agent：部署在目标集群内或其转发节点上，主动连到数据面的隧道入口，
// 把数据面的请求转给本集群的 API Server。不连数据库，不依赖 Kratos。
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lanyulei/reborn/backend/k8s/internal/agent"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Version 由 -ldflags "-X main.Version=..." 注入。
var Version = "dev"

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		serverURL  = flag.String("server-url", env("REBORN_SERVER_URL", ""), "data plane tunnel endpoint, e.g. https://k8s-tunnel.example.com")
		kubeconfig = flag.String("kubeconfig", env("REBORN_KUBECONFIG", ""), "kubeconfig of the cluster (relay mode); empty = in-cluster mode")
		stateDir   = flag.String("state-dir", env("REBORN_STATE_DIR", ""), "directory holding the agent token (relay mode)")
		namespace  = flag.String("namespace", env("REBORN_NAMESPACE", "reborn-system"), "namespace holding the agent token Secret (in-cluster mode)")
		caFile     = flag.String("ca-file", env("REBORN_CA_FILE", ""), "CA bundle to verify the tunnel endpoint; empty = system roots")
		insecure   = flag.Bool("insecure-skip-verify", env("REBORN_INSECURE_SKIP_VERIFY", "") == "true", "skip TLS verification of the tunnel endpoint (development only)")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "reborn-agent", "version", Version)
	if err := run(log, *serverURL, *kubeconfig, *stateDir, *namespace, *caFile, *insecure); err != nil {
		log.Error("agent stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, serverURL, kubeconfig, stateDir, namespace, caFile string, insecure bool) error {
	var (
		cfg   *rest.Config
		store agent.TokenStore
		err   error
		mode  = "in-cluster"
	)
	if kubeconfig != "" {
		mode = "relay"
		if stateDir == "" {
			return errors.New("relay mode requires a state directory (REBORN_STATE_DIR)")
		}
		if cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig); err != nil {
			return err
		}
		store = agent.NewFileStore(stateDir)
	} else {
		if cfg, err = rest.InClusterConfig(); err != nil {
			return err
		}
		client, cerr := kubernetes.NewForConfig(cfg)
		if cerr != nil {
			return cerr
		}
		store = agent.NewSecretStore(client, namespace)
	}
	proxy, err := agent.NewProxy(cfg, log)
	if err != nil {
		return err
	}
	a, err := agent.New(agent.Config{
		ServerURL: serverURL, RegisterToken: os.Getenv("REBORN_REGISTER_TOKEN"), Version: Version,
		CAFile: caFile, InsecureSkipVerify: insecure,
	}, store, proxy, log)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("agent starting", "mode", mode, "api_server", cfg.Host, "pid", strconv.Itoa(os.Getpid()))
	return a.Run(ctx)
}
