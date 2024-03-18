package dag

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tuannh982/dag-bft/dag/commons"
	"github.com/tuannh982/dag-bft/dag/internal"
	"github.com/tuannh982/dag-bft/utils/collections"
	"github.com/tuannh982/dag-bft/utils/service"

	log "github.com/sirupsen/logrus"
)

type FabftNode struct {
	service.SimpleService
	// node info
	nodeInfo *commons.NodeInfo
	// peers info
	peers []*FabftNode
	f     int
	w     int
	// persistent info
	dag         internal.DAG
	round       commons.Round
	buffer      collections.Set[*commons.Vertex]
	decidedWave commons.Wave
	leaderStack collections.Stack[*commons.Vertex]
	// non-persistent info
	timer             *time.Timer
	timerTimeout      time.Duration
	networkAssumption commons.NetworkAssumption
	// channels
	RBcastChannel  chan internal.BroadcastMessage[*commons.Vertex, commons.Round]
	ABcastChannel  chan internal.BroadcastMessage[commons.Block, uint64]
	BlockToPropose chan commons.Block
	// log
	log *log.Entry
}

func NewFabftNode(addr commons.Address, waveSize int, timerTimeout time.Duration) *RiderNode {
	logger := log.WithFields(log.Fields{"node": string(addr)})
	logger.Logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	logger.Level = log.InfoLevel
	instance := &RiderNode{
		nodeInfo: &commons.NodeInfo{
			Address: addr,
		},
		peers:          make([]*RiderNode, 0),
		f:              0,
		w:              waveSize,
		dag:            internal.NewDAG(),
		round:          0,
		buffer:         collections.NewHashSet(commons.VertexHash),
		decidedWave:    0,
		leaderStack:    collections.NewStack[*commons.Vertex](),
		timer:          time.NewTimer(0),
		timerTimeout:   timerTimeout,
		RBcastChannel:  make(chan internal.BroadcastMessage[*commons.Vertex, commons.Round], 65535),
		ABcastChannel:  make(chan internal.BroadcastMessage[commons.Block, uint64], 65535),
		BlockToPropose: make(chan commons.Block, 65535),
		log:            logger,
	}
	instance.peers = append(instance.peers, instance)
	_ = instance.timer.Stop()
	instance.SimpleService = *service.NewSimpleService(instance)
	return instance
}

func (node *FabftNode) OnStart(ctx context.Context) error {
	err := node.Init()
	if err != nil {
		return err
	}
	node.StartRoutine(ctx)
	return nil
}

func (node *FabftNode) Init() error {
	round0 := commons.Round(0)
	node.dag.NewRoundIfNotExists(round0)
	for _, peer := range node.peers {
		v := commons.Vertex{
			StrongEdges: make([]commons.BaseVertex, 0),
			WeakEdges:   make([]commons.BaseVertex, 0),
			Delivered:   false,
		}
		v.Source = peer.nodeInfo.Address
		v.Round = 0
		v.Block = ""
		b := node.dag.GetRound(round0).AddVertex(v)
		if !b {
			return errors.New("could not add vertex")
		}
	}
	return nil
}

func (node *FabftNode) OnStop() {
	close(node.RBcastChannel)
	close(node.ABcastChannel)
	close(node.BlockToPropose)
	if !node.timer.Stop() {
		select {
		case <-node.timer.C:
		default:
		}
	}
}

func (node *FabftNode) SetPeers(peers []*FabftNode) {
	node.peers = peers
	node.f = len(node.peers) / 3
}

func (node *FabftNode) ReportRoutine(ctx context.Context, interval time.Duration) {
	timer := time.NewTimer(interval)
	for {
		select {
		case <-timer.C:
			fmt.Println("REPORT", fmt.Sprintf("round=%d\n%s", node.round, node.dag.String()))
			timer.Reset(interval)
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (node *FabftNode) StartRoutine(ctx context.Context) {
	go node.ReportRoutine(ctx, 10*time.Second)
	go node.ReceiveRoutine(ctx)
	node.timer.Reset(node.timerTimeout)
	go node.TimeoutRoutine(ctx)
}

func (node *FabftNode) ReceiveRoutine(ctx context.Context) {
	for {
		select {
		case rMsg := <-node.RBcastChannel:
			node.log.Debug("receive message from RBcastChannel", "p=", rMsg.P, "r=", rMsg.R, "m=", rMsg.Message)
			node.rDeliver(rMsg.Message, rMsg.R, rMsg.P)
		case aMsg := <-node.ABcastChannel:
			node.log.Debug("receive message from ABcastChannel", "p=", aMsg.P, "r=", aMsg.R, "m=", aMsg.Message)
			node.aBcast(aMsg.Message, aMsg.R)
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

func (node *FabftNode) TimeoutRoutine(ctx context.Context) {
	for {
		select {
		case <-node.timer.C:
			node.log.Debug("timer timeout")
			node.handleTimeout()
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

func (node *RiderNode) handleTimeout() {
	node.log.Debug("buffer", node.buffer.Entries(), "round", node.round, "node.dag.GetRound(node.round).Size()", node.dag.GetRound(node.round).Size())
	for _, v := range node.buffer.Entries() {
		if v.Round <= node.round && node.dag.AllEdgesExist(v) {
			node.dag.NewRoundIfNotExists(v.Round)
			node.dag.GetRound(v.Round).AddVertex(*v)
			must(node.buffer.Remove(v))
			node.log.Debug("vertex added to DAG", v)
		}
	}
	if node.dag.GetRound(node.round).Size() >= 2*node.f+1 {
		if int64(node.round)%int64(node.w) == 0 {
			w := commons.Wave(int64(node.round) / int64(node.w))
			node.log.Debug("wave ready", w)
			node.waveReady(w)
		}
		node.dag.NewRoundIfNotExists(node.round + 1)
		v := node.createNewVertex(node.round + 1)
		if v != nil {
			node.rBcast(v, node.round+1)
			node.round = node.round + 1
		}
	}
	node.timer.Reset(node.timerTimeout)
}
