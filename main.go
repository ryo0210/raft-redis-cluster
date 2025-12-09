package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"raft-redis-cluster/raft"
	"raft-redis-cluster/store"
	"raft-redis-cluster/transport"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// initialPeersList nodeID->address mapping
type initialPeersList []Peer

type Peer struct {
	NodeID    string
	RaftAddr  string
	RedisAddr string
}

func (i *initialPeersList) Set(value string) error {
	node := strings.Split(value, ",")
	for _, n := range node {
		nodes := strings.Split(n, "=")
		if len(nodes) != 2 {
			return errors.New("invalid peer format. expected nodeID=raftAddr[redisAddr]")
		}

		address := strings.Split(nodes[1], "|")
		if len(address) != 2 {
			fmt.Println(address)
			return errors.New("invalid peer format. expected nodeID=raftAddress|RedisAddress")
		}

		*i = append(*i, Peer{
			NodeID:    nodes[0],
			RaftAddr:  address[0],
			RedisAddr: address[1],
		})
	}
	return nil
}

func (i *initialPeersList) String() string {
	return fmt.Sprintf("%v", *i)
}

var (
	serverID     = flag.String("server_id", "", "Node id used by Raft")
	raftAddr     = flag.String("address", "localhost:50051", "TCP host+port for this raft node")
	redisAddr    = flag.String("redis_address", "localhost:6379", "TCP host+port for redis")
	dataDir      = flag.String("data_dir", "", "Raft data dir")
	initialPeers = initialPeersList{}
)

func init() {
	flag.Var(&initialPeers, "initial_peers", "Initial peers for the Raft cluster")
	flag.Parse()
	validateFlags()
}

func validateFlags() {
	if *serverID == "" {
		log.Fatalf("flag --server_id is required")
	}

	if *raftAddr == "" {
		log.Fatalf("flag --address is required")
	}

	if *redisAddr == "" {
		log.Fatalf("flag --redis_address is required")
	}

	if *dataDir == "" {
		log.Fatalf("flag --data_dir is required")
	}
}

func main() {
	datastore := store.NewMemoryStore()
	st := raft.NewStateMachine(datastore)
	log.Println(initialPeers.String())
	r, sdb, err := NewRaft(*dataDir, *serverID, *raftAddr, st, initialPeers)
	if err != nil {
		log.Fatalln(err)
	}

	redis := transport.NewRedis(hraft.ServerID(*serverID), r, datastore, sdb)
	err = redis.Serve(*redisAddr)
	if err != nil {
		log.Fatalln(err)
	}
}

const snapshotRetainCount = 2

func NewRaft(baseDir string, id string, address string, fsm hraft.FSM, nodes initialPeersList) (*hraft.Raft, hraft.StableStore, error) {
	c := hraft.DefaultConfig()
	c.LocalID = hraft.ServerID(id)

	// raftboltdb.BoltStoreは、hashicorp/raftが提供しているLogStoreとStableStore、両方のInterfaceを実装している

	// LogStore: raftのログを保存する
	// func (b *BoltStore) FirstIndex() (uint64, error)
	// func (b *BoltStore) LastIndex() (uint64, error)
	// func (b *BoltStore) GetLog(idx uint64, raftlog *raft.Log) error
	// func (b *BoltStore) StoreLog(log *raft.Log) error
	// func (b *BoltStore) StoreLogs(logs []*raft.Log) error
	// func (b *BoltStore) DeleteRange(min uint64, max uint64) error

	// StableStore: TermやVoteを保存する
	// func (b *BoltStore) Set(k []byte, v []byte) error
	// func (b *BoltStore) Get(k []byte) ([]byte, error)
	// func (b *BoltStore) SetUint64(key []byte, val uint64) error
	// func (b *BoltStore) GetUint64(key []byte) (uint64, error)

	ldb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "raft_logs.dat"))
	if err != nil {
		return nil, nil, err
	}

	sdb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, err
	}

	fss, err := hraft.NewFileSnapshotStore(baseDir, snapshotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, nil, err
	}

	tm, err := hraft.NewTCPTransport(address, tcpAddr, 10, time.Second, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	r, err := hraft.NewRaft(c, fsm, ldb, sdb, fss, tm)
	if err != nil {
		return nil, nil, err
	}

	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{
				Suffrage: hraft.Voter,
				ID:       hraft.ServerID(id),
				Address:  hraft.ServerAddress(address),
			},
		},
	}

	for _, peer := range nodes {
		sid := hraft.ServerID(peer.NodeID)
		cfg.Servers = append(cfg.Servers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       sid,
			Address:  hraft.ServerAddress(peer.RaftAddr),
		})

		err := store.SetRedisAddrByNodeID(sdb, sid, peer.RedisAddr)
		if err != nil {
			return nil, nil, err
		}
	}

	f := r.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return nil, nil, err
	}

	return r, sdb, nil
}

// この実装だとredis clusterのシャーディング構成じゃないので、負荷分散とかは出来てない？
// いや、この構成が複数あればredis clusterになるのか？
// → 6章に書いてあったわ（Multi-Raft を⽤いたシャーディングへの道のり）
