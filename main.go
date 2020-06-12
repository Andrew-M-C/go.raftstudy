package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
	"github.com/influxdata/telegraf/agent"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// bcChans = []chan raftpb.Message{
	// 	make(chan raftpb.Message),
	// 	make(chan raftpb.Message),
	// 	make(chan raftpb.Message),
	// }
}

var (
	infof  = log.Printf
	errorf = log.Printf

	bcChans = []chan raftpb.Message{
		make(chan raftpb.Message),
		make(chan raftpb.Message),
		make(chan raftpb.Message),
	}
)

const (
	tickInterval      = 100 * time.Millisecond
	jitterMillisecond = 15 * time.Millisecond
)

func main() {
	infof("hello, raft!")
	defer infof("end of raft")

	// infof("ElectionTimeout = %v", raft.ElectionTimeout)

	peers := []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}}
	go startNode(0x01, peers)
	go startNode(0x02, peers)
	go startNode(0x03, peers)

	time.Sleep(2 * time.Second)
	return
}

func startNode(id uint64, peers []raft.Peer) {
	ctx := context.TODO()
	storage := raft.NewMemoryStorage()
	c := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	n := &node{
		id:     id,
		prefix: strings.Repeat("\t\t\t", int(id)) + "| ",
		node:   raft.StartNode(&c, peers),
		// tick:        time.NewTicker(tickInterval),
		tick:        agent.NewRollingTicker(tickInterval-jitterMillisecond, tickInterval+jitterMillisecond),
		recv:        bcChans[id-1],
		raftStorage: storage,
	}

	for {
		select {
		// case <-n.tick.C:
		case <-n.tick.Elapsed():
			n.node.Tick()
			// infof("%d - tick, status: %+v", n.id, n.node.Status())

		case rd := <-n.node.Ready():
			// infof("%d - ready: %+v", id, rd)
			// if raft.IsEmptySnap(rd.Snapshot) {
			// 	infof("%d - EmptySnap", id)
			// } else {
			// 	infof("%d - Snapshot: %+v", id, rd.Snapshot)
			// }
			n.raftStorage.Append(rd.Entries)
			go n.sendMessage(rd.Messages)
			n.node.Advance()

		case m := <-n.recv:
			infof("%d -%s got message from %v to %v, type %v", id, n.prefix, m.From, m.To, m.Type)
			n.node.Step(ctx, m)
			// b, _ := m.Marshal()
			// n.node.Propose(ctx, b)
			infof("%d -%s status: %v", id, n.prefix, n.node.Status().RaftState)

		default:
			// infof("%d - default", id)
		}
	}

	return
}

type node struct {
	id     uint64
	prefix string
	node   raft.Node
	// tick        *time.Ticker
	tick        *agent.RollingTicker
	recv        chan raftpb.Message
	raftStorage *raft.MemoryStorage
}

func (n *node) sendMessage(msg []raftpb.Message) {
	for _, m := range msg {
		to := m.To
		ch := bcChans[to-1]
		infof("%d -%s send to %v, type %v", n.id, n.prefix, m.To, m.Type)
		ch <- m
	}
	return
}
