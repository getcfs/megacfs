package oort

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/getcfs/megacfs/syndicate/utils/srvconf"
	"github.com/gholt/ring"
	"github.com/pandemicsyn/cmdctrl"
)

// FExists true if a file or dir exists
func FExists(name string) bool {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false
	}
	return true
}

type ccc struct {
	CmdCtrlConfig cmdctrl.ConfigOpts
}

func (o *Server) loadCmdCtrlConfig() error {
	config := ccc{}
	err := o.LoadRingConfig(&config)
	o.CmdCtrlConfig = config.CmdCtrlConfig
	return err
}

func (o *Server) LoadRingConfig(config interface{}) (err error) {
	o.Lock()
	defer o.Unlock()
	e := NewEnvGetter(fmt.Sprintf("OORT_%s", strings.ToUpper(o.serviceName)), "_")
	localConfig := e.Get("LOCAL_CONFIG")
	if localConfig != "" {
		_, err = toml.DecodeFile(localConfig, config)
		if err != nil {
			return err
		}
	}
	log.Println("Using ring version:", o.ring.Version())
	b := bytes.NewReader(o.ring.Config())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, config)
		if err != nil {
			return err
		}
	}
	// Now overlay per node config on top
	n := o.ring.LocalNode()
	if n == nil {
		panic("n is nil")
	}
	b = bytes.NewReader(o.ring.LocalNode().Config())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, config)
		if err != nil {
			return err
		}
	}
	log.Printf("Local Node config is: \n%s", o.ring.LocalNode().Config())
	log.Printf("Ring config is: \n%s", o.ring.Config())
	return nil
}

type EnvGetter struct {
	pre string
	sep string
}

func NewEnvGetter(prefix, seperator string) *EnvGetter {
	return &EnvGetter{pre: prefix, sep: seperator}
}

func (e *EnvGetter) Get(varname string) string {
	return os.Getenv(fmt.Sprintf("%s%s%s", e.pre, e.sep, varname))
}

func (o *Server) ObtainConfig() (err error) {
	e := NewEnvGetter(fmt.Sprintf("OORT_%s", strings.ToUpper(o.serviceName)), "_")
	envSkipSRV := e.Get("SKIP_SRV")
	// Check whether we're supposed to skip loading via srv method
	if strings.ToLower(envSkipSRV) != "true" {
		s := &srvconf.SRVLoader{
			SyndicateURL: e.Get("SYNDICATE_OVERRIDE"),
		}
		s.Record, err = GenServiceID(o.serviceName, "syndicate", "tcp")
		if err != nil {
			if e.Get("SYNDICATE_OVERRIDE") == "" {
				log.Println(err)
			} else {
				log.Fatalln("No SYNDICATE_OVERRIDE provided and", err)
			}
		}
		if e.Get("SYNDICATE_OVERRIDE") != "" {
			log.Println("Over wrote syndicate url with url from env!", e.Get("SYNDICATE_OVERRIDE"))
		}
		nc, err := s.Load()
		if err != nil {
			return err
		}
		o.ring, err = ring.LoadRing(bytes.NewReader(nc.Ring))
		if err != nil {
			return fmt.Errorf("Error while loading ring for config get via srv lookup: %s", err)
		}
		err = ring.PersistRingOrBuilder(o.ring, nil, fmt.Sprintf("%s/ring/%d-%s.ring", o.cwd, o.ring.Version(), o.serviceName))
		if err != nil {
			return err
		}
		o.localID = nc.Localid
		o.ring.SetLocalNode(o.localID)
		o.ringFile = fmt.Sprintf("%s/ring/%d-%s.ring", o.cwd, o.ring.Version(), o.serviceName)
		err = o.loadCmdCtrlConfig()
		if err != nil {
			return err
		}
	} else {
		// if you skip the srv load you have to provide all of the info in env vars!
		log.Println("Skipped SRV Config attempting to load from env")
		s, err := strconv.ParseUint(e.Get("LOCALID"), 10, 64)
		if err != nil {
			return fmt.Errorf("Unable to load env specified local id")
		}
		o.localID = s
		o.ringFile = e.Get("RING_FILE")
		o.ring, _, err = ring.RingOrBuilder(o.ringFile)
		if err != nil {
			return fmt.Errorf("Unable to road env specified ring: %s", err)
		}
		o.ring.SetLocalNode(o.localID)
		err = o.loadCmdCtrlConfig()
		if err != nil {
			return err
		}
	}
	return nil
}

//TODO: need to remove the hack to add IAD3 identifier -- What hack? Not sure
// what this refers to.
func GenServiceID(service, name, proto string) (string, error) {
	h, _ := os.Hostname()
	d := strings.SplitN(h, ".", 2)
	if len(d) != 2 {
		return "", fmt.Errorf("Unable to determine FQDN, only got short name.")
	}
	return fmt.Sprintf("_%s-%s._%s.%s", service, name, proto, d[1]), nil
}

func GetRingServer(servicename string) (string, error) {
	// All-In-One defaults
	h, _ := os.Hostname()
	d := strings.SplitN(h, ".", 2)
	if strings.HasSuffix(d[0], "-aio") {
		// TODO: Not sure about name, proto -- are those ever *not* "syndicate"
		// and "tcp"?
		switch servicename {
		case "value":
			return h + ":8443", nil
		case "group":
			return h + ":8444", nil
		}
		panic("Unknown service " + servicename)
	}
	service, err := GenServiceID(servicename, "syndicate", "tcp")
	if err != nil {
		return "", err
	}
	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("Syndicate SRV lookup is empty")
	}
	return fmt.Sprintf("%s:%d", addrs[0].Target, addrs[0].Port), nil
}
