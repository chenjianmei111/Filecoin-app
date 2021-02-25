package types

import (
	"github.com/ipfs/go-cid"
)

const (
	//filecoin30秒为一个纪元
	EpochDurationSeconds = 30

	//默认计价起始时间48H
	DefaultDealStartOffset = 48 * 60 * 60 / EpochDurationSeconds

	//最短需要存储6个月
	MinDealDuration = 180 * (24 * 60 * 60 / EpochDurationSeconds)
)

type StorageDealConfig struct {
	Miner string
	//pricr per Gib/Epoch
	Price           uint64
	FastRetrieval   bool
	DealStartOffset int64
}
type StoreResult struct {
	ProposalCid cid.Cid
	Config      StorageDealConfig
	Success     bool
	Message     string
}

// StorageAsk has information about an active ask from a storage miner.
type StorageAsk struct {
	Miner        string
	Price        uint64
	MinPieceSize uint64
	MaxPieceSize uint64
	Timestamp    int64
	Expiry       int64
}

// StorageDealInfo contains information about a proposed storage deal
type StorageDealInfo struct {
	ProposalCid cid.Cid
	StateID     uint64
	StateName   string
	Miner       string

	PieceCID cid.Cid
	Size     uint64

	PricePerEpoch uint64
	StartEpoch    uint64
	Duration      uint64

	DealID          uint64
	ActivationEpoch int64
	Message         string
}

// StorageDealRecord represents a storage deal log record.
type StorageDealRecord struct {
	RootCid  cid.Cid
	Addr     string
	DealInfo StorageDealInfo
	Time     int64
	Pending  bool
}
