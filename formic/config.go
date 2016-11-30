package formic

import (
	"log"
	"os"
	"strconv"
)

type config struct {
	Path string
	Port int
	//	fsPort                     int
	OortValueSyndicate         string
	OortGroupSyndicate         string
	InsecureSkipVerify         bool
	SkipMutualTLS              bool
	NodeID                     int
	MetricsAddr                string
	MetricsCollectors          string
	PoolSize                   int
	ConcurrentRequestsPerStore int
	Debug                      bool
	GRPCMetrics                bool
}

func ResolveConfig(c *config) *config {
	cfg := &config{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("FORMICD_PATH"); env != "" {
		cfg.Path = env
	}
	if cfg.Path == "" {
		cfg.Path = "/var/lib/formic"
	}
	if env := os.Getenv("FORMICD_PORT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.Port = val
		}
	}
	if cfg.Port == 0 {
		cfg.Port = 8445
	}
	if env := os.Getenv("FORMICD_OORT_VALUE_SYNDICATE"); env != "" {
		log.Println("Value: ", env)
		cfg.OortValueSyndicate = env
	}
	// cfg.oortValueSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_OORT_GROUP_SYNDICATE"); env != "" {
		log.Println("Group: ", env)
		cfg.OortGroupSyndicate = env
	}
	// cfg.oortGroupSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_INSECURE_SKIP_VERIFY"); env == "true" {
		cfg.InsecureSkipVerify = true
	}
	if env := os.Getenv("FORMICD_SKIP_MUTUAL_TLS"); env == "true" {
		cfg.SkipMutualTLS = true
	}
	if env := os.Getenv("FORMICD_NODE_ID"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.NodeID = val
		}
	}
	cfg.MetricsAddr = ":9100"
	if env := os.Getenv("FORMICD_METRICS_ADDR"); env != "" {
		cfg.MetricsAddr = env
	}
	if env := os.Getenv("FORMICD_METRICS_COLLECTORS"); env != "" {
		cfg.MetricsCollectors = env
	}
	if env := os.Getenv("FORMICD_POOL_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PoolSize = val
		}
	}
	if env := os.Getenv("FORMICD_CONCURRENT_REQUESTS_PER_STORE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.ConcurrentRequestsPerStore = val
		}
	}
	cfg.Debug = false
	if env := os.Getenv("FORMICD_DEBUG"); env == "true" {
		cfg.Debug = true
	}
	cfg.GRPCMetrics = true
	if env := os.Getenv("FORMICD_GRPC_METRICS"); env == "false" {
		cfg.GRPCMetrics = false
	}

	return cfg
}
