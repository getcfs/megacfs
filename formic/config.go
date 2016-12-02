package formic

import (
	"os"
	"strconv"

	"github.com/gholt/ring"
)

type Config struct {
	Path                       string
	Port                       int
	InsecureSkipVerify         bool
	SkipMutualTLS              bool
	NodeID                     int
	MetricsAddr                string
	MetricsCollectors          string
	PoolSize                   int
	ConcurrentRequestsPerStore int
	Debug                      bool
	GRPCMetrics                bool

	FormicAddressIndex int
	GroupAddressIndex  int
	ValueAddressIndex  int
	CertFile           string
	KeyFile            string
	CAFile             string
	Ring               ring.Ring
	RingPath           string
	IpAddr             string

	AuthUrl      string
	AuthUser     string
	AuthPassword string
}

func ResolveConfig(c *Config) *Config {
	cfg := &Config{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("AUTH_URL"); env != "" {
		cfg.AuthUrl = env
	}
	if env := os.Getenv("AUTH_USER"); env != "" {
		cfg.AuthUser = env
	}
	if env := os.Getenv("AUTH_PASSWORD"); env != "" {
		cfg.AuthPassword = env
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
