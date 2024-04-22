package dag

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/exp/rand"
	"strconv"
	"time"

	"github.com/mitchellh/hashstructure"
	"github.com/tuannh982/dag-bft/dag/commons"
	"github.com/tuannh982/dag-bft/dag/internal"
	"github.com/tuannh982/dag-bft/utils/collections"
	"github.com/tuannh982/dag-bft/utils/service"

	log "github.com/sirupsen/logrus"
)

type FabftNode struct {
	service.SimpleService
	// node info
	NodeInfo *commons.NodeInfo
	// peers info
	peers    []*FabftNode
	peersMap collections.Map[commons.Address, *FabftNode]
	f        int
	// persistent info
	dag               internal.FabftDAG
	round             commons.Round
	buffer            collections.Map[commons.VHash, *commons.Vertex]                // storing received blocks with no qc
	bufferWaitingPrev collections.Map[commons.VHash, *commons.Vertex]                // buffer2, storing blocks with QC but missing prev blocks
	missingPrev       collections.Map[commons.VHash, collections.Set[commons.VHash]] // map hash of vertices in buffer2 to their missing prev blocks
	// non-persistent info
	timer             *time.Timer
	timerTimeout      time.Duration
	networkAssumption commons.NetworkAssumption
	// channels
	VertexChannel  chan internal.BroadcastMessage[commons.Vertex, commons.Round]
	VoteChannel    chan commons.Vote
	BlockToPropose chan commons.Block
	QCChannel      chan commons.QC
	signatures     []collections.Set[string]
	// log
	log *log.Entry
}

func NewFabftNode(addr commons.Address, networkAssumption commons.NetworkAssumption, timerTimeout time.Duration) *FabftNode {
	logger := log.WithFields(log.Fields{"node": string(addr)})
	logger.Logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	logger.Level = log.InfoLevel
	instance := &FabftNode{
		NodeInfo: &commons.NodeInfo{
			Address: addr,
		},
		peers:             make([]*FabftNode, 0),
		f:                 0,
		dag:               internal.NewFabftDAG(),
		round:             0,
		buffer:            collections.NewHashMap[commons.VHash, *commons.Vertex](),
		bufferWaitingPrev: collections.NewHashMap[commons.VHash, *commons.Vertex](),
		missingPrev:       collections.NewHashMap[commons.VHash, collections.Set[commons.VHash]](),
		timer:             time.NewTimer(0),
		timerTimeout:      timerTimeout,
		VertexChannel:     make(chan internal.BroadcastMessage[commons.Vertex, commons.Round], 65535),
		BlockToPropose:    make(chan commons.Block, 65535),
		log:               logger,
		networkAssumption: networkAssumption,
		peersMap:          collections.NewHashMap[commons.Address, *FabftNode](),
		QCChannel:         make(chan commons.QC, 128),
		signatures:        make([]collections.Set[string], 0),
		VoteChannel:       make(chan commons.Vote, 1024),
	}
	instance.peers = append(instance.peers, instance)
	_ = instance.timer.Stop()
	instance.SimpleService = *service.NewSimpleService(instance)
	return instance
}

func (node *FabftNode) SetPeers(peers []*FabftNode) {
	node.peers = peers
	node.f = len(node.peers) / 3
	for _, p := range peers {
		err := node.peersMap.Put(p.NodeInfo.Address, p, true)
	}
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
		v.Source = peer.NodeInfo.Address
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
	close(node.VertexChannel)
	close(node.BlockToPropose)
	close(node.QCChannel)
	close(node.VoteChannel)
	if !node.timer.Stop() {
		select {
		case <-node.timer.C:
		default:
		}
	}
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
		/*
			case rMsg := <-node.RBcastChannel:
				node.log.Debug("receive message from RBcastChannel", "p=", rMsg.P, "r=", rMsg.R, "m=", rMsg.Message)
				node.rDeliver(rMsg.Message, rMsg.R, rMsg.P)
		*/
		case vertexMsg := <-node.VertexChannel:
			node.log.Debug("receive message from VertexChannel", "p=", vertexMsg.P, "r=", vertexMsg.R, "m=", vertexMsg.Message)
			go node.verifyVertexMsg(&vertexMsg)
		case vMsg := <-node.VoteChannel:
			node.log.Debug("receive message from VoteChannel", "approved=", vMsg.Approved, "signature=", vMsg.Signature, "hash=", vMsg.Hash, "round=", vMsg.Hash)
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

func (node *FabftNode) TimeoutRoutine(ctx context.Context) {
	for {
		if node.networkAssumption == commons.PartiallySynchronous {
			select {
			case <-node.timer.C:
				node.log.Debug("timer timeout")
				node.handleTimeout()
			case <-ctx.Done():
				return
			default:
				continue
			}
		} else {

		}
	}
}

func (node *FabftNode) handleTimeout() {
	go node.checkQC()
	node.log.Debug("buffer", node.buffer.Entries(), "round", node.round, "node.dag.GetRound(node.round).Size()", node.dag.GetRound(node.round).Size())

	/*for _, v := range node.buffer.Entries() {
		if v.Round <= node.round && node.dag.AllEdgesExist(v) {
			node.dag.NewRoundIfNotExists(v.Round)
			node.dag.GetRound(v.Round).AddVertex(*v)
			must(node.buffer.Remove(v))
			node.log.Debug("vertex added to DAG", v)
		}
	}*/

	b := node.generateVertex()
	node.vertexBcast(b, node.round) // broadcast block to all peers

	/*if node.dag.GetRound(node.round).Size() >= 2*node.f+1 {
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
	}*/
	node.timer.Reset(node.timerTimeout)
}

func (node *FabftNode) generateVertex() *commons.Vertex {
	block := "transaction placeholder" + strconv.Itoa(rand.Int()) // placeholder, to be placed with real blocks with transactions
	bv := commons.BaseVertex{
		Source: node.NodeInfo.Address,
		Round:  node.round,
		Block:  block,
	}
	if node.round == 0 {
		v := &commons.Vertex{
			BaseVertex:  bv,
			StrongEdges: make([]commons.BaseVertex, 0),
			WeakEdges:   nil,
			Delivered:   false,
			PrevHashes:  make([]commons.VHash, 0),
			VertexHash:  0,
		}
		vh, err := hashstructure.Hash(v, nil)
		v.VertexHash = commons.VHash(vh)
		return v
	}

	roundSet := node.dag.GetRound(node.round - 1)
	var hashPointers []uint64
	for _, tips := range roundSet.Entries() {
		hashPointers = append(hashPointers, tips.VertexHash)
	}
	v := &commons.Vertex{
		BaseVertex:  bv,
		StrongEdges: nil,
		WeakEdges:   nil,
		Delivered:   false,
		PrevHashes:  hashPointers,
	}
	vh, err := hashstructure.Hash(v, nil)
	v.VertexHash = vh
	return v
}

func (node *FabftNode) rBcast(v *commons.Vertex, r commons.Round) {
	for _, peer := range node.peers {
		clonedPeer := peer
		go func() {
			node.log.Debug("message rBcast to", clonedPeer.NodeInfo.Address, "v=", v, "r=", r)
			clonedPeer.RBcastChannel <- internal.BroadcastMessage[*commons.Vertex, commons.Round]{
				Message: v,
				R:       r,
				P:       node.NodeInfo.Address,
			}
		}()
	}
}

func (node *FabftNode) verifyVertexMsg(vm *internal.BroadcastMessage[commons.Vertex, commons.Round]) {
	if node.verifyVertex(&vm.Message) {
		sig := node.signVertex(&vm.Message)
		node.vote(true, sig, &vm.Message)
	}
	node.buffer.Put(vm.Message.VertexHash, &vm.Message, true)
}

func (node *FabftNode) verifyVertex(v *commons.Vertex) bool {
	// placeholder, need further implementation
	return true
}

func (node *FabftNode) vertexBcast(v *commons.Vertex, r commons.Round) {
	for _, peer := range node.peers {
		if peer.NodeInfo.Address != node.NodeInfo.Address {
			clonedPeer := peer
			go func() {
				node.log.Println("message vertexBcast to", clonedPeer.NodeInfo.Address, "v=", v, "r=", r)
				clonedPeer.VertexChannel <- internal.BroadcastMessage[commons.Vertex, commons.Round]{
					Message: *v,
					R:       r,
					P:       node.NodeInfo.Address,
				}
			}()
		}
	}
}

func (node *FabftNode) checkQC() {
	for qc := range node.QCChannel {
		v, err := node.buffer.Get(qc.VertexHash)
		lastRoundVertices := node.dag.GetRound(v.Round - 1).VertexMap()
		for _, ph := range v.PrevHashes {
			if _, ok := lastRoundVertices[ph]; !ok {

				node.missingPrev.Put(ph)
			}
		}
	}
}

func (node *FabftNode) vote(approved bool, signature string, v *commons.Vertex) {
	vSource, err := node.peersMap.Get(v.Source)
	vSource.VoteChannel <- commons.Vote{
		Round:     v.Round,
		Approved:  approved,
		Signature: signature,
		Hash:      v.VertexHash,
	}
}

func (node *FabftNode) signVertex(v *commons.Vertex) string {
	// placeholder
	return "signature"
}
