package main

import (
	"context"
	"log"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	bcChans = []chan pb.Message{
		make(chan pb.Message),
		make(chan pb.Message),
		make(chan pb.Message),
	}
}

var (
	infof  = log.Printf
	errorf = log.Printf

	bcChans = []chan pb.Message{}
)

const (
	tickMillisecond = 20
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
		id:          id,
		node:        raft.StartNode(&c, peers),
		tick:        time.NewTicker(tickMillisecond * time.Millisecond),
		recv:        bcChans[id-1],
		raftStorage: storage,
	}

	go n.receive()

	for {
		select {
		case <-n.tick.C:
			n.node.Tick()
			// infof("%d - tick, status: %+v", n.id, n.node.Status())

		case rd := <-n.node.Ready():
			// infof("%d - ready: %+v", id, rd)
			if raft.IsEmptySnap(rd.Snapshot) {
				infof("%d - EmptySnap", id)
			} else {
				infof("%d - Snapshot: %+v", id, rd.Snapshot)
			}
			n.raftStorage.Append(rd.Entries)
			go sendMessage(id, rd.Messages)
			n.node.Advance()

		case m := <-n.recv:
			infof("%d - got message from %v to %v, type %v", id, m.From, m.To, m.Type)
			b, _ := m.Marshal()
			n.node.Propose(ctx, b)
			infof("%d - status: %v", id, n.node.Status().RaftState)

		default:
			// infof("%d - default", id)
		}
	}

	return
}

type node struct {
	id          uint64
	node        raft.Node
	tick        *time.Ticker
	recv        chan pb.Message
	raftStorage *raft.MemoryStorage
}

func sendMessage(id uint64, msg []pb.Message) {
	for _, m := range msg {
		to := m.To
		ch := bcChans[to-1]
		ch <- m
		infof("%d - send to %d done", id, to)
	}
	return
}

func (n *node) receive() {
	return
}
