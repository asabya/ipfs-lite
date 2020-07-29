package main

// This example launches an IPFS-Lite peer and fetches a hello-world
// hash from the IPFS network.

import (
	"context"
	"fmt"
	"github.com/asabya/ipfs-lite/config"
	"github.com/asabya/ipfs-lite/repo"
	"io/ioutil"
	"os"

	ipfslite "github.com/asabya/ipfs-lite"
	"github.com/ipfs/go-cid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := "/tmp" + string(os.PathSeparator) + repo.Root
	conf, err := config.ConfigInit(2048)
	if err != nil {
		return
	}
	err = repo.Init(root, conf)
	if err != nil {
		return
	}

	r, err := repo.Open(root)
	if err != nil {
		return
	}

	lite, err := ipfslite.New(ctx, r)
	if err != nil {
		panic(err)
	}

	lite.Bootstrap(ipfslite.DefaultBootstrapPeers())

	c, _ := cid.Decode("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	rsc, err := lite.GetFile(ctx, c)
	if err != nil {
		panic(err)
	}
	defer rsc.Close()
	content, err := ioutil.ReadAll(rsc)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(content))
}
