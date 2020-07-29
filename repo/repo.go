// Most of the code is taken from go-ipfs
// https://github.com/ipfs/go-ipfs/blob/master/repo/fsrepo/fsrepo.go

package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/asabya/ipfs-lite/config"
	"github.com/facebookgo/atomicfile"
	lockfile "github.com/ipfs/go-fs-lock"
	logger "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
)

const Root = ".idrop"

type Repo interface {
	Config() (*config.Config, error)
	Path() string
}

var log = logger.Logger("DropRepo")

var (
	packageLock sync.Mutex
)

const LockFile = "repo.lock"

type FSRepo struct {
	// has Close been called already
	closed bool
	// path is the file-system path
	path string
	// lockfile is the file system lock to prevent others from opening
	// the same fsrepo path concurrently
	lockfile io.Closer
	config   *config.Config
}

func (r *FSRepo) Config() (*config.Config, error) {
	// It is not necessary to hold the package lock since the repo is in an
	// opened state. The package lock is _not_ meant to ensure that the repo is
	// thread-safe. The package lock is only meant to guard against removal and
	// coordinate the lockfile. However, we provide thread-safety to keep
	// things simple.
	packageLock.Lock()
	defer packageLock.Unlock()

	if r.closed {
		return nil, errors.New("cannot access config, repo not open")
	}
	return r.config, nil
}

func (r *FSRepo) Path() string {
	return r.path
}

func Init(repoPath string, conf *config.Config) error {
	// packageLock must be held to ensure that the repo is not initialized more
	// than once.
	packageLock.Lock()
	defer packageLock.Unlock()

	// Check if already initialised
	if isInitializedUnsynced(repoPath) {
		return nil
	}
	log.Debug("calling initConfig with conf :", conf)
	if err := initConfig(repoPath, conf); err != nil {
		return err
	}

	return nil
}

// Open the FSRepo at path. Returns an error if the repo is not
// initialized.
func Open(repoPath string) (Repo, error) {
	return open(repoPath)
}

func open(repoPath string) (Repo, error) {
	packageLock.Lock()
	defer packageLock.Unlock()

	r, err := newFSRepo(repoPath)
	if err != nil {
		return nil, err
	}

	// Check if its initialized
	//if err := checkInitialized(r.path); err != nil {
	//	return nil, err
	//}

	r.lockfile, err = lockfile.Lock(r.path, LockFile)
	if err != nil {
		return nil, err
	}
	keepLocked := false
	defer func() {
		// unlock on error, leave it locked on success
		if !keepLocked {
			r.lockfile.Close()
		}
	}()

	if err := r.openConfig(); err != nil {
		return nil, err
	}

	return r, nil
}

func newFSRepo(rpath string) (*FSRepo, error) {
	expPath, err := homedir.Expand(filepath.Clean(rpath))
	if err != nil {
		return nil, err
	}

	return &FSRepo{path: expPath}, nil
}

func initConfig(path string, cfg *config.Config) error {
	configFilename, err := config.Filename(path)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	err = os.MkdirAll(filepath.Dir(configFilename), 0775)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	f, err := atomicfile.New(configFilename, 0660)
	if err != nil {
		return err
	}
	defer f.Close()

	return encode(f, cfg)
}

// encode configuration with JSON
func encode(w io.Writer, value interface{}) error {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	buf, err := config.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// openConfig returns an error if the config file is not present.
func (r *FSRepo) openConfig() error {
	configFilename, err := config.Filename(r.path)
	if err != nil {
		return err
	}
	conf := config.Config{}
	f, err := os.Open(configFilename)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&conf); err != nil {
		return fmt.Errorf("failure to decode config: %s", err)
	}
	r.config = &conf
	return nil
}

// configIsInitialized returns true if the repo is initialized at
// provided |path|.
func configIsInitialized(path string) bool {
	configFilename, err := config.Filename(path)
	if err != nil {
		return false
	}

	if !FileExists(configFilename) {
		return false
	}
	return true
}

// isInitializedUnsynced reports whether the repo is initialized. Caller must
// hold the packageLock.
func isInitializedUnsynced(repoPath string) bool {
	return configIsInitialized(repoPath)
}

// FileExists check if the file with the given path exits.
func FileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !os.IsNotExist(err)) {
		return true
	}
	return false
}