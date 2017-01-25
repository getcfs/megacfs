package formic

import (
	"os"
	"strconv"

	"github.com/gholt/ring"
)

type Config struct {
	NodeID int
	Debug  bool

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
	SkipAuth     bool
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
	cfg.NodeID = -1
	if env := os.Getenv("FORMIC_NODE_ID"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.NodeID = val
		}
	}
	cfg.Debug = false
	if env := os.Getenv("DEBUG"); env == "true" {
		cfg.Debug = true
	}
	cfg.SkipAuth = false
	if env := os.Getenv("SKIP_AUTH"); env == "true" {
		cfg.SkipAuth = true
	}

	return cfg
}
