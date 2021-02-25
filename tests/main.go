package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/huaqian/application/lotus"
	"github.com/huaqian/application/storage"
	"github.com/huaqian/application/types"
	"github.com/ipfs/go-datastore"
	"io/ioutil"
	"os"
	"time"
)

const (
	//addr      = "47.254.230.220:1234"
	//	token      = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.E-kJLVsvHjthJcao6sdYg7_r3rzxrNSbeop6eQOqGuY"
	//mineraddr  = "f082617"
	//clientaddr = "f1nphk7muey5zpdgacrk5fxrks7xcf6rpikkxikxy"
	mineraddr  = "t0112831"
	clientaddr = "t1newyw5dw3japj7ylfaxiasjlv67gh7gd7l6la5a"
	token      = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.uFBPIBJiJZPGbGKwRMDuZ7YowRTqFlZV6k4EKsnWvbs"
	addr       = "127.0.0.1:1234"
)

func storagedata(ctx context.Context, builder lotus.ClientBuilder, data []byte, duration uint64) (*types.StoreResult, error) {
	if data == nil {
		return nil, fmt.Errorf("not data to storage!")
	}
	api, cls, err := builder(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating lotus client error:%s", err)
	}
	defer cls()
	m := &storage.Sto{
		Lapi:                api,
		Ds:                  datastore.NewMapDatastore(),
		PollDuration:        time.Second * 2,
		DealFinalityTimeout: time.Minute * 1,
	}
	cid, size, err := m.Import(ctx, bytes.NewReader(data), false)
	if err != nil {
		return nil, fmt.Errorf("error when Import data:%s", err)
	}
	fmt.Printf("cid=%s,size=%d,\n", cid, size)
	ask, bol, err := m.GetMinerStorageAsk(ctx, mineraddr)
	if err != nil || bol != true {
		return nil, fmt.Errorf("error when Get Miner StorageAsk:%s", err)
	}
	fmt.Printf("Miner:%s,Price:%d,MinPieceSize:%d,MaxPieceSize:%d,Timestamp:%d,Expiry:%d\n", ask.Miner, ask.Price, ask.MinPieceSize, ask.MaxPieceSize, ask.Timestamp, ask.Expiry)
	cfg := types.StorageDealConfig{
		Miner: ask.Miner,
		Price: ask.Price,
	}
	res, err := m.Store(ctx, clientaddr, cid, cfg, duration)
	if err != nil {
		return nil, fmt.Errorf("error when Store data:%s", err)
	}
	fmt.Printf("store ProposalCid:%s,miner addr:%s\n", res.ProposalCid, res.Config.Miner)
	select {
	case <-ctx.Done():
	}
	return res, nil
}
func main() {
	builder, err := lotus.NewBuilder(token, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create lotus builder Error:%s", err.Error())
	}
	//	Filename := "/data/application/tests/2020-12-29_05-33.png"
	Filename := "/home/cjm/filecoin/application/tests/2020-12-29_05-33.png"
	data, err := ioutil.ReadFile(Filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "File Error: %s\n", err)
	}
	res, err := storagedata(context.Background(), builder, data, types.MinDealDuration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storagedata Error: %s\n", err.Error())
	} else {
		fmt.Printf("Store data succes,store ProposalCid:%s,miner addr:%s,DealID:%d\n", res.ProposalCid, res.Config.Miner)
	}
}
