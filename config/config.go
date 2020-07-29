// Most of the code is taken from go-ipfs-config
// https://github.com/ipfs/go-ipfs-config/blob/a5ce8d10b52673d8633c350e620d181c5ec15e23/config.go

package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	// DefaultConfigFile is the filename of the configuration file
	DefaultConfigFile = "config"

	SwarmPort = "3700"
)

var (
	defaultReprovideInterval = 12 * time.Hour
)

// Identity tracks the configuration of the local node's identity.
type Identity struct {
	PeerID  string
	PrivKey string `json:",omitempty"`
}

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	Swarm []string // addresses for the swarm to listen on
}

type ConnMgr struct {
	LowWater  int
	HighWater int
}

// Config wraps configuration options for the Peer.
type Config struct {
	Identity   Identity  // local node's peer identity
	Addresses  Addresses // local node's addresses
	Bootstrap  []string
	DeviceName string
	ConnMgr    ConnMgr
	// ReprovideInterval sets how often to reprovide records to the DHT
	ReprovideInterval time.Duration
}

// Filename returns the configuration file path given a configuration root
// directory. If the configuration root directory is empty, use the default one
func Filename(configroot string) (string, error) {
	return configroot + string(os.PathSeparator) + DefaultConfigFile, nil
}

// Marshal configuration with JSON
func Marshal(value interface{}) ([]byte, error) {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	return json.MarshalIndent(value, "", "  ")
}

func FromMap(v map[string]interface{}) (*Config, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	var conf Config
	if err := json.NewDecoder(buf).Decode(&conf); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}
	return &conf, nil
}

func ToMap(conf *Config) (map[string]interface{}, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(conf); err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.NewDecoder(buf).Decode(&m); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}
	return m, nil
}

func ConfigInit(nbits int) (*Config, error) {
	identity, err := identityConfig(nbits)
	if err != nil {
		return nil, err
	}
	conf := &Config{
		Addresses:         addressesConfig(),
		Bootstrap:         nil,
		Identity:          identity,
		ReprovideInterval: defaultReprovideInterval,
		ConnMgr:           connMgrConfig(),
	}

	return conf, nil
}

func identityConfig(nbits int) (Identity, error) {
	ident := Identity{}

	sk, pk, err := ci.GenerateKeyPair(ci.Ed25519, nbits)
	if err != nil {
		return ident, err
	}

	skbytes, err := sk.Bytes()
	if err != nil {
		return ident, err
	}
	ident.PrivKey = base64.StdEncoding.EncodeToString(skbytes)

	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return ident, err
	}
	ident.PeerID = id.Pretty()
	return ident, nil
}

func addressesConfig() Addresses {
	return Addresses{
		Swarm: []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", SwarmPort),
			fmt.Sprintf("/ip6/::/tcp/%s", SwarmPort),
		},
	}
}

func connMgrConfig() ConnMgr {
	return ConnMgr{
		HighWater: 20,
		LowWater:  3,
	}
}