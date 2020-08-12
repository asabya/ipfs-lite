package main

// This example launches an IPFS-Lite peer and fetches a hello-world
// hash from the IPFS network.

import (
	"context"
	"fmt"
	"os"

	ipfslite "github.com/asabya/ipfs-lite"
	"github.com/asabya/ipfs-lite/config"
	"github.com/asabya/ipfs-lite/repo"
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

	n, err := lite.AddDir(context.Background(), "/home/sabyasachi/go/src/gitlab.com/diplay-uploader/resources/video/Game of Thrones Season 8 Official Trailer (HBO)/hls", nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(n.Cid())
	for i, j := range n.Links() {
		fmt.Println(i, j.Cid, j.Name)
	}
	fmt.Println(n.Size())
	select{ }
}
