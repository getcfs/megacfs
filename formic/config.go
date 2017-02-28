package formic

import (
	"os"
	"strconv"

	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
)

type Config struct {
	NodeID int

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

func NewConfig() *Config {
	return &Config{
		NodeID: -1,
	}
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
	if env := os.Getenv("FORMIC_NODE_ID"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.NodeID = val
		}
	}
	if brimtext.TrueString(os.Getenv("SKIP_AUTH")) {
		cfg.SkipAuth = true
	}
	if brimtext.FalseString(os.Getenv("SKIP_AUTH")) {
		cfg.SkipAuth = false
	}

	return cfg
}
