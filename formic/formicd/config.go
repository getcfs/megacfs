package main

import (
	"log"
	"os"
	"strconv"
)

type config struct {
	path string
	port int
	//	fsPort                     int
	oortValueSyndicate         string
	oortGroupSyndicate         string
	insecureSkipVerify         bool
	skipMutualTLS              bool
	nodeId                     int
	metricsAddr                string
	metricsCollectors          string
	concurrentRequestsPerStore int
	debug                      bool
}

func resolveConfig(c *config) *config {
	cfg := &config{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("FORMICD_PATH"); env != "" {
		cfg.path = env
	}
	if cfg.path == "" {
		cfg.path = "/var/lib/formic"
	}
	if env := os.Getenv("FORMICD_PORT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.port = val
		}
	}
	if cfg.port == 0 {
		cfg.port = 8445
	}
	if env := os.Getenv("FORMICD_OORT_VALUE_SYNDICATE"); env != "" {
		log.Println("Value: ", env)
		cfg.oortValueSyndicate = env
	}
	// cfg.oortValueSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_OORT_GROUP_SYNDICATE"); env != "" {
		log.Println("Group: ", env)
		cfg.oortGroupSyndicate = env
	}
	// cfg.oortGroupSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_INSECURE_SKIP_VERIFY"); env == "true" {
		cfg.insecureSkipVerify = true
	}
	if env := os.Getenv("FORMICD_SKIP_MUTUAL_TLS"); env == "true" {
		cfg.skipMutualTLS = true
	}
	if env := os.Getenv("FORMICD_NODE_ID"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.nodeId = val
		}
	}
	cfg.metricsAddr = ":9100"
	if env := os.Getenv("FORMICD_METRICS_ADDR"); env != "" {
		cfg.metricsAddr = env
	}
	if env := os.Getenv("FORMICD_METRICS_COLLECTORS"); env != "" {
		cfg.metricsCollectors = env
	}
	if env := os.Getenv("FORMICD_CONCURRENT_REQUESTS_PER_STORE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.concurrentRequestsPerStore = val
		}
	}
	if env := os.Getenv("FORMICD_DEBUG"); env == "true" {
		cfg.debug = true
	}
	return cfg
}
