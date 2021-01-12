package main

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
	"github.com/influxdata/telegraf/agent"
)

func init() {
	// log.SetFlags(log.Lshortfile | log.LstdFlags)

	// bcChans = []chan raftpb.Message{
	// 	make(chan raftpb.Message, 100),
	// 	make(chan raftpb.Message, 100),
	// 	make(chan raftpb.Message, 100),
	// }
}

var (
	bcChans = []chan raftpb.Message{
		make(chan raftpb.Message, 100),
		make(chan raftpb.Message, 100),
		make(chan raftpb.Message, 100),
		make(chan raftpb.Message, 100),
	}

	dataChans = []chan string{
		make(chan string, 100),
		make(chan string, 100),
		make(chan string, 100),
		make(chan string, 100),
	}

	entryChans = []chan raftpb.Entry{
		make(chan raftpb.Entry, 100),
		make(chan raftpb.Entry, 100),
		make(chan raftpb.Entry, 100),
		make(chan raftpb.Entry, 100),
	}

	confChangeChans = []chan raftpb.ConfChange{
		make(chan raftpb.ConfChange, 100),
		make(chan raftpb.ConfChange, 100),
		make(chan raftpb.ConfChange, 100),
		make(chan raftpb.ConfChange, 100),
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
	go startNode(0x01, peers, false)
	go startNode(0x02, peers, false)

	time.Sleep(time.Second)
	go startNode(0x03, peers, false)
	time.Sleep(2 * time.Second)

	// send data
	dataChans[1] <- "MSGETCD"
	time.Sleep(time.Second)

	// add node
	peers = append(peers, raft.Peer{ID: 0x04})
	go startNode(0x04, peers, true)
	time.Sleep(2 * time.Second)

	// send data
	dataChans[1] <- "MSGRAFT"
	time.Sleep(2 * time.Second)

	return
}

func startNode(id uint64, peers []raft.Peer, shouldAddNode bool) {
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
		prefix: strings.Repeat("\t\t", int(id)) + strconv.Itoa(int(id)) + " - ",
		node:   raft.StartNode(&c, peers),
		// tick:        time.NewTicker(tickInterval),
		tick:        agent.NewRollingTicker(tickInterval-jitterMillisecond, tickInterval+jitterMillisecond),
		recv:        bcChans[id-1],
		msg:         dataChans[id-1],
		entry:       entryChans[id-1],
		confChange:  confChangeChans[id-1],
		raftStorage: storage,
		nodeIDMap:   map[uint64]bool{},
	}

	for _, peer := range peers {
		n.nodeIDMap[peer.ID] = true
	}

	if shouldAddNode {
		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: id,
		}
		infof("%d -%s now ProposeConfChange: %+v", id, n.prefix, cc)

		for i, ch := range confChangeChans {
			if uint64(i) == id-1 {
				continue
			}
			go func(i int, ch chan raftpb.ConfChange) {
				infof("%d -%s send confChange to %d", id, n.prefix, i+1)
				ch <- cc
			}(i, ch)
			break
		}
	}

	for {
		select {
		// case <-n.tick.C:
		case <-n.tick.Elapsed():
			n.node.Tick()

		case rd := <-n.node.Ready():
			// infof("%d -%s ready: %+v", id, n.prefix, rd)
			infof("%d -%s node.Ready --> Entries count %d, CommittedEntries count %d",
				id, n.prefix, len(rd.Entries), len(rd.CommittedEntries),
			)
			if false == raft.IsEmptySnap(rd.Snapshot) {
				infof("%d -%s is NOT empty snapshot", id, n.prefix)
			}
			for _, en := range rd.CommittedEntries {
				infof("%d -%s got CommittedEntries: %v, %v, %s (%d)", id, n.prefix, en.Index, en.Type, en.Data, len(en.Data))
				switch en.Type {
				case raftpb.EntryConfChange:
					cc := raftpb.ConfChange{}
					err := cc.Unmarshal(en.Data)
					if err != nil {
						errorf("%d -%s unmarshal EntryConfChange error: %v", id, n.prefix, err)
					} else {
						infof("%d -%s got EntryConfChange: %+v", id, n.prefix, cc)
						confState := n.node.ApplyConfChange(cc)
						infof("%d -%s ApplyConfChange done, confState: %v", id, n.prefix, confState)
						if cc.NodeID == id {
							infof("%d -%s I am recognized by group", id, n.prefix)
						}
					}
				default:
					infof("%d -%s got CommittedEntries type: %v", id, n.prefix, en.Type)
				}
			}

			for _, en := range rd.Entries {
				if len(en.Data) == 0 {
					infof("%d -%s got entry: %v, %v, heartbeat", id, n.prefix, en.Index, en.Type)
				} else {
					infof(
						"%d -%s got entry: %v, %v, data <%v> <%s>, len (%d)",
						id, n.prefix, en.Index, en.Type, hex.EncodeToString(en.Data), string(en.Data), len(en.Data),
					)
				}
			}
			n.raftStorage.Append(rd.Entries)
			if lastIdx, err := n.raftStorage.LastIndex(); err != nil {
				errorf("%d -%s read LastIndex error: %v", err)
			} else {
				infof("%d -%s LastIndex: %d", id, n.prefix, lastIdx)
			}

			go n.sendMessage(rd.Messages)

			infof("%d -%s IsEmptyHardState() = %v", id, n.prefix, raft.IsEmptyHardState(rd.HardState))
			n.node.Advance()

		case cc := <-n.confChange:
			infof("%d -%s confChange --> request add node %d", id, n.prefix, cc.NodeID)
			err := n.node.ProposeConfChange(ctx, cc)
			if err != nil {
				errorf("%d -%s ProposeConfChange error: %v", id, n.prefix, err)
			}

		case msg := <-n.msg:
			infof("%d -%s propose --> got external message request: '%s'", id, n.prefix, msg)
			err := n.node.Propose(ctx, []byte(msg))
			if err != nil {
				errorf("%d -%s Propose error: %v", id, n.prefix, err)
			}

		case m := <-n.recv:
			infof(
				"%d -%s received rpc --> got message from %v to %v, type %v, entry count %d",
				id, n.prefix, m.From, m.To, m.Type, len(m.Entries),
			)
			// if len(m.Entries) > 0 {
			// 	n.raftStorage.Append(m.Entries)
			// }
			for i, e := range m.Entries {
				infof("%d -%s received rpc --> Entry %d: type %v", id, n.prefix, i, e.Type)
			}
			n.node.Step(ctx, m)
			infof("%d -%s status: %v", id, n.prefix, n.node.Status().RaftState)
			if lastIdx, err := n.raftStorage.LastIndex(); err != nil {
				errorf("%d -%s read LastIndex error: %v", err)
			} else {
				infof("%d -%s LastIndex: %d", id, n.prefix, lastIdx)
			}

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
	msg         chan string
	entry       chan raftpb.Entry
	confChange  chan raftpb.ConfChange
	raftStorage *raft.MemoryStorage
	nodeIDMap   map[uint64]bool
}

func (n *node) sendMessage(msg []raftpb.Message) {
	infof("%d -%s got %d message(s) to send", n.id, n.prefix, len(msg))
	for _, m := range msg {
		to := m.To
		ch := bcChans[to-1]
		// infof("%d -%s msg %d - got %d entries to send", n.id, n.prefix, i, len(m.Entries))

		infof(
			"%d -%s send from %d to %v, type %v, term %d, Index %d",
			n.id, n.prefix, m.From, m.To, m.Type, m.Term, m.Index,
		)
		for _, entry := range m.Entries {
			infof(
				"%d -%s send from %d to %v, type %v, entry type %v, term %d, Index %d, Data len %d, Data text <%s> <%s>",
				n.id, n.prefix, m.From, m.To, m.Type, entry.Type, m.Term, m.Index,
				len(entry.Data), string(entry.Data), hex.EncodeToString(entry.Data),
			)
		}
		ch <- m
	}
	return
}
