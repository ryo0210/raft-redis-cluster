package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"raft-redis-cluster/raft"
	"raft-redis-cluster/store"
	"strings"
	"time"

	"github.com/tidwall/redcon"

	hraft "github.com/hashicorp/raft"
)

type Redis struct {
	listen      net.Listener
	store       store.Store
	stableStore hraft.StableStore
	id          hraft.ServerID
	raft        *hraft.Raft
}

func NewRedis(id hraft.ServerID, raft *hraft.Raft, store store.Store, stableStore hraft.StableStore) *Redis {
	return &Redis{
		id:          id,
		raft:        raft,
		store:       store,
		stableStore: stableStore,
	}
}

func (r *Redis) Serve(addr string) error {
	var err error
	r.listen, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return r.handle()
}

func (r *Redis) handle() error {
	return redcon.Serve(r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			err := r.validateCmd(cmd)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			r.processCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
			if err != nil {
				log.Default().Println("error:", err)
			}
		},
	)
}

var argsLen = map[string]int{
	"GET": 2,
	"SET": 3,
	"DEL": 2,
}

const (
	commandName = 0
	keyName     = 1
	value       = 2
)

var ErrArgsLen = errors.New("Err wrong number of arguments for command")

func (r *Redis) validateCmd(cmd redcon.Command) error {
	if len(cmd.Args) == 0 {
		return ErrArgsLen
	}

	// この処理はいるのか？
	if len(cmd.Args) < argsLen[string(cmd.Args[commandName])] {
		return ErrArgsLen
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	if len(cmd.Args) != argsLen[plainCmd] {
		return ErrArgsLen
	}

	return nil
}

func (r *Redis) processCmd(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	// Raftではリーダーのみがリクエストを受け付ける
	// もし受け取った場合、3つのパターンがある
	// 1. 自分はリーダーではないとエラーを返す
	// 2. 代わりにリーダーに聞きにいく
	// 3. クライアントにリーダーのアドレスを返して、再送するようにする
	// この本では3番を実装している
	if r.raft.State() != hraft.Leader {
		_, lid := r.raft.LeaderWithID()
		add, err := store.GetRedisAddrByNodeID(r.stableStore, lid)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		conn.WriteError("MOVED -1 " + add)
		return
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	switch plainCmd {
	case "GET":
		// GETの場合はraftを使わず、直接store経由で取って来れる。
		// コマンドをレプリケーションする必要がないので。
		val, err := r.store.Get(ctx, cmd.Args[keyName])
		if err != nil {
			switch {
			case errors.Is(err, store.ErrKeyNotFound):
				conn.WriteNull()
				return
			default:
				conn.WriteError(err.Error())
				return
			}
		}
		conn.WriteBulk(val)
	case "SET":
		kvCmd := &raft.KVCmd{
			Op:    raft.Put,
			Key:   cmd.Args[keyName],
			Value: cmd.Args[value],
		}

		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}

		conn.WriteString("OK")
	case "DEL":
		kvCmd := &raft.KVCmd{
			Op:    raft.Del,
			Key:   cmd.Args[keyName],
			Value: cmd.Args[value],
		}

		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}

		// Note that if FSM.Apply returns an error, it will be returned by Response,
		// and not by the Error method, so it is always important to check Response
		// for errors from the FSM.
		// FSM.Applyがエラーを返した場合、そのエラーはErrorメソッドではなくResponseによって返されることに注意してください。
		// したがって、FSMからのエラーを確認する際には常にResponseをチェックすることが重要です。
		//
		// putの方でf.Response()をやってないのはなんで？
		res := f.Response()
		err, ok := res.(error)
		if ok {
			conn.WriteError(err.Error())
			return
		}

		conn.WriteInt(1)
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[commandName]) + "'")
	}
}

func (r *Redis) Close() error {
	return r.listen.Close()
}

func (r *Redis) Addr() net.Addr {
	return r.listen.Addr()
}
