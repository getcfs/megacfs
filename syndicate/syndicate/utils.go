package syndicate

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func Filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func getRingPaths(cfg *Config, servicename string) (lastBuilder string, lastRing string, err error) {
	bstring := filepath.Join(cfg.RingDir, fmt.Sprintf("%s.builder", servicename))
	rstring := filepath.Join(cfg.RingDir, fmt.Sprintf("%s.ring", servicename))
	_, err = os.Stat(bstring)
	if err != nil {
		//TODO: no active builder found, so should we search for the most recent one
		//we can find and load it and hopefully its matching ring?
		return "", "", fmt.Errorf("No builder file found in %s, looking for: %s", cfg.RingDir, bstring)
	}
	lastBuilder = bstring

	_, err = os.Stat(rstring)
	if err != nil {
		//TODO: if we don't find a matching oort.ring should we just
		// use oort.builder to make new one ?
		return "", "", fmt.Errorf("No ring file found in %s, looking for: %s", cfg.RingDir, rstring)
	}
	lastRing = rstring
	return lastBuilder, lastRing, nil
}

func findLastRing(cfg *Config) (lastBuilder string, lastRing string, err error) {
	fp, err := os.Open(cfg.RingDir)
	if err != nil {
		return "", "", err
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		return "", "", err
	}

	fn := Filter(names, func(v string) bool {
		return strings.HasSuffix(v, "-oort.builder")
	})
	sort.Strings(fn)
	if len(fn) != 0 {
		lastBuilder = filepath.Join(cfg.RingDir, fn[len(fn)-1])
	}

	fn = Filter(names, func(v string) bool {
		return strings.HasSuffix(v, "-oort.ring")
	})
	if len(fn) != 0 {
		lastRing = filepath.Join(cfg.RingDir, fn[len(fn)-1])
	}
	return lastBuilder, lastRing, nil
}

func ExtractCapacity(path string, disks []*pb.Disk) uint32 {
	for k := range disks {
		if disks[k] != nil {
			if disks[k].Path == path {
				return uint32(disks[k].Size_ / 1024 / 1024 / 1024)
			}
		}
	}
	return 0
}
