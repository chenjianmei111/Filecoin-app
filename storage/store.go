package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	typ "github.com/filecoin-project/lotus/chain/types"
	"github.com/huaqian/application/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/textileio/powergate/util"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

type Sto struct {
	Lapi                *apistruct.FullNodeStruct
	Ds                  datastore.Datastore
	lock                sync.Mutex
	PollDuration        time.Duration
	DealFinalityTimeout time.Duration
}

var (
	dsBaseStoragePending = datastore.NewKey("storage-pending")
	dsBaseStorageFinal   = datastore.NewKey("storage-final")
	log                  = types.InitLogger()
	chanWriteTimeout     = time.Second
)

func (m *Sto) Import(ctx context.Context, data io.Reader, isCAR bool) (cid.Cid, int64, error) {
	//f, err := ioutil.TempFile("/data/application/tempfile", "import-*")
	f, err := ioutil.TempFile("/home/cjm/filecoin/application/tempfile", "import-*")
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("error when creating tmpfile: %s", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Errorf("closing storing file: %s", err)
		}
	}()
	var size int64
	if size, err = io.Copy(f, data); err != nil {
		return cid.Undef, 0, fmt.Errorf("error when copying data to tmpfile: %s", err)
	}
	ref := api.FileRef{
		Path:  f.Name(),
		IsCAR: isCAR,
	}
	res, err := m.Lapi.ClientImport(ctx, ref)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("error when importing data: %s", err)
	}
	return res.Root, size, nil
}

func (m *Sto) GetMinerStorageAsk(ctx context.Context, addrStr string) (types.StorageAsk, bool, error) {
	addr, err := address.NewFromString(addrStr)
	if err != nil {
		return types.StorageAsk{}, false, fmt.Errorf("miner address is invalid: %s", err)
	}
	power, err := m.Lapi.StateMinerPower(ctx, addr, typ.EmptyTSK)
	if err != nil {
		return types.StorageAsk{}, false, fmt.Errorf("getting power %s: %s", addr, err)
	}
	if power.MinerPower.RawBytePower.IsZero() {
		return types.StorageAsk{}, false, fmt.Errorf("Miner`s power cant not be zero!")
	}
	mi, err := m.Lapi.StateMinerInfo(ctx, addr, typ.EmptyTSK)
	if err != nil {
		return types.StorageAsk{}, false, fmt.Errorf("getting miner %s info: %s", addr, err)
	}

	if mi.PeerId == nil {
		return types.StorageAsk{}, false, fmt.Errorf("Miner`s PeerId cant not be zero!")
	}

	sask, err := m.Lapi.ClientQueryAsk(ctx, *mi.PeerId, addr)
	if err != nil {
		return types.StorageAsk{}, false, fmt.Errorf("Client Query Ask:%s", err)
	}
	return types.StorageAsk{
		Miner:        sask.Miner.String(),
		Price:        sask.Price.Uint64(),
		MinPieceSize: uint64(sask.MinPieceSize),
		MaxPieceSize: uint64(sask.MaxPieceSize),
		Timestamp:    int64(sask.Timestamp),
		Expiry:       int64(sask.Expiry),
	}, true, nil
}

func (m *Sto) Store(ctx context.Context, waddr string, dataCid cid.Cid, cfg types.StorageDealConfig, minDuration uint64) (*types.StoreResult, error) {
	if minDuration < types.MinDealDuration {
		return nil, fmt.Errorf("duration %d should be greater or equal to %d", minDuration, types.MinDealDuration)
	}
	addr, err := address.NewFromString(waddr)
	if err != nil {
		return nil, fmt.Errorf("parsing wallet address: %s", err)
	}
	ts, err := m.Lapi.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chaing head: %s", err)
	}
	maddr, err := address.NewFromString(cfg.Miner)
	if err != nil {
		log.Errorf("invalid miner address :%s", err)
	}
	if cfg.DealStartOffset == 0 {
		cfg.DealStartOffset = types.DefaultDealStartOffset
	}
	dataCIDSize, err := m.Lapi.ClientDealPieceCID(ctx, dataCid)
	if err != nil {
		return nil, fmt.Errorf("error when get pieceCID:%s", err)
	}
	params := &api.StartDealParams{
		Data: &storagemarket.DataRef{
			TransferType: storagemarket.TTGraphsync,
			Root:         dataCid,
			PieceSize:    dataCIDSize.PieceSize.Unpadded(),
			PieceCid:     &dataCIDSize.PieceCID,
		},
		MinBlocksDuration: minDuration,
		EpochPrice:        big.Div(big.Mul(big.NewIntUnsigned(cfg.Price), big.NewIntUnsigned(uint64(dataCIDSize.PieceSize))), abi.NewTokenAmount(1<<30)),
		Miner:             maddr,
		Wallet:            addr,
		FastRetrieval:     cfg.FastRetrieval,
		DealStartEpoch:    ts.Height() + abi.ChainEpoch(cfg.DealStartOffset),
	}
	p, err := m.Lapi.ClientStartDeal(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("starting deal with :%s", err)
	}
	res := &types.StoreResult{
		Config:      cfg,
		ProposalCid: *p,
		Success:     true,
	}
	log.Infof("store ProposalCid:%s,miner addr:%s\n", res.ProposalCid, res.Config.Miner)
	m.storageDealSendData(params, *p)
	return res, nil
}
func (m *Sto) storageDealSendData(params *api.StartDealParams, proposalCid cid.Cid) {
	di := types.StorageDealInfo{
		Duration:      params.MinBlocksDuration,
		PricePerEpoch: params.EpochPrice.Uint64(),
		Miner:         params.Miner.String(),
		ProposalCid:   proposalCid,
	}
	record := types.StorageDealRecord{
		RootCid:  params.Data.Root,
		Addr:     params.Wallet.String(),
		Time:     time.Now().Unix(),
		DealInfo: di,
		Pending:  true,
	}
	log.Infof("storing pending deal record for proposal cid: %s", util.CidToString(proposalCid))
	if err := m.putPendingDeal(record); err != nil {
		log.Errorf("storing pending deal: %s", err)
		return
	}
	go m.eventuallyFinalizeDeal(record, m.DealFinalityTimeout)
}

func (m *Sto) putPendingDeal(dr types.StorageDealRecord) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	buf, err := json.Marshal(dr)
	if err != nil {
		return fmt.Errorf("marshaling PendingDeal: %s", err)
	}
	if err := m.Ds.Put(makePendingDealKey(dr.DealInfo.ProposalCid), buf); err != nil {
		return fmt.Errorf("put PendingDeal: %s", err)
	}
	return nil
}

func (m *Sto) deletePendingDeal(proposalCid cid.Cid) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if err := m.Ds.Delete(makePendingDealKey(proposalCid)); err != nil {
		return fmt.Errorf("delete PendingDeal: %s", err)
	}
	return nil
}

func (m *Sto) putFinalDeal(dr types.StorageDealRecord) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	err := m.Ds.Delete(makePendingDealKey(dr.DealInfo.ProposalCid))
	if err != nil {
		return fmt.Errorf("deleting PendingDeal: %s", err)
	}
	buf, err := json.Marshal(dr)
	if err != nil {
		return fmt.Errorf("marshaling DealRecord: %s", err)
	}
	if err := m.Ds.Put(makeFinalDealKey(dr.DealInfo.ProposalCid), buf); err != nil {
		return fmt.Errorf("put DealRecord: %s", err)
	}
	return nil
}

func (m *Sto) eventuallyFinalizeDeal(dr types.StorageDealRecord, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	updates, err := m.Watch(ctx, dr.DealInfo.ProposalCid)
	if err != nil {
		log.Errorf("watching proposal cid %s: %v", util.CidToString(dr.DealInfo.ProposalCid), err)
		return
	}
	for {
		log.Info("deal waiting.......")
		select {
		case <-ctx.Done():
			log.Infof("watching proposal cid %s timed out, deleting pending deal", util.CidToString(dr.DealInfo.ProposalCid))
			if err := m.deletePendingDeal(dr.DealInfo.ProposalCid); err != nil {
				fmt.Errorf("deleting pending deal: %v", err)
			}
			return
		case info, ok := <-updates:
			if !ok {
				log.Errorf("updates channel unexpectedly closed for proposal cid %s", util.CidToString(dr.DealInfo.ProposalCid))
				if err := m.deletePendingDeal(dr.DealInfo.ProposalCid); err != nil {
					log.Errorf("deleting pending deal: %v", err)
				}
				return
			}
			log.Infof("dealInfo updates,info.StateID= %d;storage.Market= %d", info.StateID, storagemarket.StorageDealActive)
			if info.StateID == storagemarket.StorageDealActive {
				record := types.StorageDealRecord{
					RootCid:  dr.RootCid,
					Addr:     dr.Addr,
					Time:     time.Now().Unix(),
					DealInfo: info,
					Pending:  false,
				}
				log.Infof("proposal cid %s is active, storing deal record", util.CidToString(info.ProposalCid))
				if err := m.putFinalDeal(record); err != nil {
					log.Errorf("storing proposal cid %s deal record: %v", util.CidToString(info.ProposalCid), err)
				}
				return
			} else if info.StateID == storagemarket.StorageDealProposalNotFound ||
				info.StateID == storagemarket.StorageDealProposalRejected ||
				info.StateID == storagemarket.StorageDealFailing {
				if err := m.deletePendingDeal(info.ProposalCid); err != nil {
					log.Errorf("deleting pending deal: %v", err)
				}
				return
			}
		}
	}
}

func (m *Sto) Watch(ctx context.Context, proposals cid.Cid) (<-chan types.StorageDealInfo, error) {
	if util.CidToString(proposals) == "" {
		return nil, fmt.Errorf("proposals list can't be empty")
	}
	ch := make(chan types.StorageDealInfo)
	go func() {
		defer close(ch)

		makeNotify := func() error {
			if err := m.notifyChanges(ctx, proposals, ch); err != nil {
				return fmt.Errorf("pushing new proposal states: %s", err)
			}
			return nil
		}

		// Notify once so that subscribers get a result quickly
		if err := makeNotify(); err != nil {
			log.Errorf("make notify error: %s", err)
			return

		}

		// Then notify every m.pollDuration
		for {
			res, _ := m.Lapi.ClientListDataTransfers(ctx)
			for _, chnl := range res {
				fmt.Printf("TransferID=%d;Status=%d;BaseCID=%s\n", chnl.TransferID, chnl.Status, chnl.BaseCID)
			}
			log.Info("watching deal.......")
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.PollDuration):
				if err := makeNotify(); err != nil {
					log.Errorf("creating lotus client and notifying: %s", err)
					return
				}
			}
		}
	}()
	return ch, nil
}
func (m *Sto) notifyChanges(ctx context.Context, proposals cid.Cid, ch chan<- types.StorageDealInfo) error {
	dinfo, err := m.StoreClientGetDealInfo(ctx, proposals)
	if err != nil {
		return fmt.Errorf("getting deal proposal info : %s", err)
	}
	newState, err := m.fromLotusDealInfo(ctx, dinfo)
	if err != nil {
		return fmt.Errorf("get DealInfo from Lotus error: %v", err)
	}
	select {
	case <-ctx.Done():
		return nil
	case ch <- newState:
	case <-time.After(chanWriteTimeout):
	}
	return nil
}

func (m *Sto) fromLotusDealInfo(ctx context.Context, dinfo *api.DealInfo) (types.StorageDealInfo, error) {
	di := types.StorageDealInfo{
		ProposalCid:   dinfo.ProposalCid,
		StateID:       dinfo.State,
		StateName:     storagemarket.DealStates[dinfo.State],
		Miner:         dinfo.Provider.String(),
		PieceCID:      dinfo.PieceCID,
		Size:          dinfo.Size,
		PricePerEpoch: dinfo.PricePerEpoch.Uint64(),
		Duration:      dinfo.Duration,
		DealID:        uint64(dinfo.DealID),
		Message:       dinfo.Message,
	}
	if dinfo.State == storagemarket.StorageDealActive {
		ocd, err := m.Lapi.StateMarketStorageDeal(ctx, dinfo.DealID, typ.EmptyTSK)
		if err != nil {
			log.Errorf("getting on-chain deal info: %s", err)
		}
		di.ActivationEpoch = int64(ocd.State.SectorStartEpoch)
		di.StartEpoch = uint64(ocd.Proposal.StartEpoch)
	}
	return di, nil
}

func (m *Sto) StoreClientGetDealInfo(ctx context.Context, propCid cid.Cid) (*api.DealInfo, error) {
	info, err := m.Lapi.ClientGetDealInfo(ctx, propCid)
	if err != nil {
		return nil, fmt.Errorf("get deal info error: %s", err)
	}
	if info.DealID != 0 && info.State != storagemarket.StorageDealActive {
		smd, err := m.Lapi.StateMarketStorageDeal(ctx, info.DealID, typ.EmptyTSK)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return info, nil
			}
			return nil, fmt.Errorf("state market storage deal: %s", err)
		}
		if smd.State.SectorStartEpoch > 0 {
			info.State = storagemarket.StorageDealActive
		}
	}
	return info, nil
}
func makePendingDealKey(c cid.Cid) datastore.Key {
	return dsBaseStoragePending.ChildString(util.CidToString(c))
}
func makeFinalDealKey(c cid.Cid) datastore.Key {
	return dsBaseStorageFinal.ChildString(util.CidToString(c))
}
