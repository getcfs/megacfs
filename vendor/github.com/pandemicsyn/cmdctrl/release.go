package cmdctrl

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/inconshreveable/go-update"
)

func NewGithubUpdater(repo, org, binName, target, canaryFile, currentVersion string) *GithubUpdater {
	return &GithubUpdater{
		repo:       repo,
		org:        org,
		binName:    binName,
		target:     target,
		canaryFile: canaryFile,
		version:    currentVersion,
	}
}

type GithubUpdater struct {
	sync.RWMutex
	repo       string
	org        string
	binName    string
	target     string
	canaryFile string
	version    string
}

// WriteCanary writes out the canary file so future instances
// know we're currently running a working instance.
func (g *GithubUpdater) WriteCanary() error {
	return nil
}

// RemoveCanary removes the canary file so future instances
// of the application know the upgrade is broken and the binary
// should roll back.
func (g *GithubUpdater) RemoveCanary() error {
	return nil
}

// CanaryCheck check's to see if the canary file exists.
// If it does not we'll try to roll back.
func (g *GithubUpdater) CanaryCheck() error {
	return nil
}

//Upgrade to a new version
func (g *GithubUpdater) Upgrade(version string) error {
	g.Lock()
	defer g.Unlock()
	url := fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/%s", g.org, g.repo, version, g.binName)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	opts := update.Options{
		TargetPath:  g.target,
		OldSavePath: fmt.Sprintf("%s.%s.%d", g.target, g.version, time.Now().Unix()),
	}
	err = update.Apply(resp.Body, opts)
	if err != nil {
		if rerr := update.RollbackError(err); rerr != nil {
			fmt.Printf("Failed to rollback from bad update: %v\n", rerr)
		}
	}
	return err
}

//Downgrade technically identical to upgrade
func (g *GithubUpdater) Downgrade(version string) error {
	g.Lock()
	defer g.Unlock()
	url := fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/%s", g.org, g.repo, version, g.binName)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	opts := update.Options{
		TargetPath:  g.target,
		OldSavePath: fmt.Sprintf("%s.%s.%d", g.target, g.version, time.Now().Unix()),
	}
	err = update.Apply(resp.Body, opts)
	if err != nil {
		if rerr := update.RollbackError(err); rerr != nil {
			fmt.Printf("Failed to rollback from bad update: %v\n", rerr)
		}
	}
	return err
}

//GetCurrentVersion returns the current version string
func (g *GithubUpdater) GetCurrentVersion() string {
	g.RLock()
	defer g.RUnlock()
	return g.version
}
