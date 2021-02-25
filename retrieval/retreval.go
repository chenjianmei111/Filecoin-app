package retreval

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

type Ret struct {
	Lapi *apistruct.FullNodeStruct
}

func (m *Ret) Retrieve(ctx context.Context, waddr string, payloadCid cid.Cid, pieceCid *cid.Cid, miners []string, CAREncoding bool) (string, io.ReadCloser, error) {
	rf, err := ioutil.TempDir("/home/cjm/filecoin/application/tempfile", "retrieve-*")
	//rf, err := ioutil.TempDir("/data/application/tempfile", "retrieve-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp dir for retrieval: %s", err)
	}
	ref := api.FileRef{
		Path:  filepath.Join(rf, "ret"),
		IsCAR: CAREncoding,
	}
	miner, events, err := m.retrieve(ctx, waddr, payloadCid, pieceCid, miners, &ref)
	if err != nil {
		return "", nil, fmt.Errorf("retrieving from lotus: %s", err)
	}
	//对收到的数据判断是否正确!
	for e := range events {
		if e.Err != "" {
			return "", nil, fmt.Errorf("in progress retrieval error: %s", e.Err)
		}
	}
	f, err := os.Open(ref.Path)
	if err != nil {
		return "", nil, fmt.Errorf("opening retrieved file: %s", err)
	}
	return miner, &autodeleteFile{File: f}, nil
}
func (m *Ret) retrieve(ctx context.Context, waddr string, payloadCid cid.Cid, pieceCid *cid.Cid, miners []string, ref *api.FileRef) (string, <-chan marketevents.RetrievalEvent, error) {
	addr, err := address.NewFromString(waddr)
	if err != nil {
		return "", nil, fmt.Errorf("parsing wallet address: %s", err)
	}

	// Ask each miner about costs and information about retrieving this data.
	var offers []api.QueryOffer
	for _, mi := range miners {
		a, err := address.NewFromString(mi)
		if err != nil {
			log.Infof("parsing miner address: %s", err)
		}
		qo, err := m.Lapi.ClientMinerQueryOffer(ctx, a, payloadCid, pieceCid)
		if err != nil {
			log.Infof("asking miner %s query-offer failed: %s", m, err)
			continue
		}
		offers = append(offers, qo)
	}

	// If no miners available, fail.
	if len(offers) == 0 {
		return "", nil, ErrRetrievalNoAvailableProviders
	}

	// Sort received options by price.
	sort.Slice(offers, func(a, b int) bool { return offers[a].MinPrice.LessThan(offers[b].MinPrice) })

	out := make(chan marketevents.RetrievalEvent, 1)
	var events <-chan marketevents.RetrievalEvent

	// Try with sorted miners until we got in the process of receiving data.
	var o api.QueryOffer
	for _, o = range offers {
		events, err = m.Lapi.ClientRetrieveWithEvents(ctx, o.Order(addr), ref)
		if err != nil {
			log.Infof("fetching/retrieving cid %s from %s: %s", payloadCid, o.Miner, err)
			continue
		}
		break
	}
	//上面的循环，目的是跟可以读取数据的最便宜的矿工进行交易。

	go func() {
		defer lapiCls()
		defer close(out)
		// Redirect received events to the output channel
		var errored, canceled bool
	Loop:
		for {
			select {
			case <-ctx.Done():
				log.Infof("in progress retrieval canceled")
				canceled = true
				break Loop
			case e, ok := <-events:
				if !ok {
					break Loop
				}
				if e.Err != "" {
					log.Infof("in progress retrieval errored: %s", err)
					errored = true
				}
				out <- e
			}
		}
		// Only register retrieval if successful.注册检索事件
		if !errored && !canceled {
			m.recordRetrieval(waddr, o)
		}
	}()

	return o.Miner.String(), out, nil
}
func (m *Ret) recordRetrieval(addr string, offer api.QueryOffer) {
	rr := deals.RetrievalDealRecord{
		Addr: addr,
		Time: time.Now().Unix(),
		DealInfo: deals.RetrievalDealInfo{
			RootCid:                 offer.Root,
			Size:                    offer.Size,
			MinPrice:                offer.MinPrice.Uint64(),
			Miner:                   offer.Miner.String(),
			MinerPeerID:             offer.MinerPeer.ID.String(),
			PaymentInterval:         offer.PaymentInterval,
			PaymentIntervalIncrease: offer.PaymentIntervalIncrease,
		},
	}
	if err := m.putRetrieval(rr); err != nil {
		log.Errorf("storing retrieval: %v", err)
	}
}
func (s *Ret) putRetrieval(rr deals.RetrievalDealRecord) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	buf, err := json.Marshal(rr)
	if err != nil {
		return fmt.Errorf("marshaling RetrievalRecord: %s", err)
	}
	if err := s.ds.Put(makeRetrievalKey(rr), buf); err != nil {
		return fmt.Errorf("put RetrievalRecord: %s", err)
	}
	return nil
}
