package raft

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"raft-redis-cluster/store"

	"github.com/hashicorp/raft"
)

var _ raft.FSM = (*StateMachine)(nil)

type Op int

const (
	Put Op = iota
	Del
)

type KVCmd struct {
	Op    Op     `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func NewStateMachine(store store.Store) *StateMachine {
	return &StateMachine{store}
}

// 以下の3つのメソッドを実装することで、hashicorp/raftで利用可能なStateMachineとなる。
// Apply: raftのログを受け取ってそのコマンドを実行し、データストアに適用する。
// Snapshot
// Restore
type StateMachine struct {
	store store.Store
}

func (s *StateMachine) Apply(log *raft.Log) any {
	ctx := context.Background()
	c := KVCmd{}

	err := json.Unmarshal(log.Data, &c)
	if err != nil {
		return err
	}

	return s.handleRequest(ctx, c)
}

func (s *StateMachine) Restore(rc io.ReadCloser) error {
	return s.store.Restore(rc)
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}

	return &KVSnapshot{rc}, nil
}

var ErrUnknownOp = errors.New("unknown op")

func (s *StateMachine) handleRequest(ctx context.Context, cmd KVCmd) error {
	switch cmd.Op {
	case Put:
		return s.store.Put(ctx, cmd.Key, cmd.Value)
	case Del:
		return s.store.Delete(ctx, cmd.Key)
	default:
		return ErrUnknownOp
	}
}
