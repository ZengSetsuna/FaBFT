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
	n        int
	// persistent info
	dag                    internal.FabftDAG
	round                  commons.Round
	buffer                 collections.Map[commons.VHash, *commons.Vertex]    // storing received blocks with no qc
	bufferWaitingAncestors collections.Map[commons.VHash, *commons.Vertex]    // buffer2, storing blocks with QC but missing prev blocks
	descendantsWaiting     collections.Map[commons.VHash, chan commons.VHash] // map hash of missing vertices to vertices that needs this vertex
	missingAncestorsCount  collections.IncrementableMap[commons.VHash]        // map vertices to the num of ancestors missing. add vertex to dag if this count hits 0
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
		peers:                  make([]*FabftNode, 0),
		f:                      0,
		dag:                    internal.NewFabftDAG(),
		round:                  0,
		n:                      0,
		buffer:                 collections.NewHashMap[commons.VHash, *commons.Vertex](),
		bufferWaitingAncestors: collections.NewHashMap[commons.VHash, *commons.Vertex](),
		descendantsWaiting:     collections.NewHashMap[commons.VHash, chan commons.VHash](),
		missingAncestorsCount:  collections.NewIncrementableMap[commons.VHash](),
		timer:                  time.NewTimer(0),
		timerTimeout:           timerTimeout,
		VertexChannel:          make(chan internal.BroadcastMessage[commons.Vertex, commons.Round], 65535),
		BlockToPropose:         make(chan commons.Block, 65535),
		log:                    logger,
		networkAssumption:      networkAssumption,
		peersMap:               collections.NewHashMap[commons.Address, *FabftNode](),
		QCChannel:              make(chan commons.QC, 1024),
		signatures:             make([]collections.Set[string], 0),
		VoteChannel:            make(chan commons.Vote, 1024),
	}
	instance.peers = append(instance.peers, instance)
	_ = instance.timer.Stop()
	instance.SimpleService = *service.NewSimpleService(instance)
	return instance
}

func (node *FabftNode) SetPeers(peers []*FabftNode) {
	node.peers = peers
	node.n = len(node.peers)
	node.f = node.n / 3
	for _, p := range peers {
		_ = node.peersMap.Put(p.NodeInfo.Address, p, true)
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
		addrInt64, _ := strconv.Atoi(string(peer.NodeInfo.Address))
		v := commons.Vertex{
			StrongEdges: make([]commons.BaseVertex, 0),
			WeakEdges:   make([]commons.BaseVertex, 0),
			Delivered:   false,
			PrevHashes:  make([]commons.VHash, 0),
			VertexHash:  commons.VHash(addrInt64),
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
	if node.networkAssumption == commons.PartiallySynchronous {
		node.timer.Reset(node.timerTimeout)
		go node.TimeoutRoutine(ctx)
	} else {
		go node.AsynchronousRoutine(ctx)
	}
	go node.receiveQC(ctx)
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
		/*case vMsg := <-node.VoteChannel:
		node.log.Debug("receive message from VoteChannel", "approved=", vMsg.Approved, "signature=", vMsg.Signature, "hash=", vMsg.Hash, "round=", vMsg.Hash)
		go node.receiveVote */
		case <-ctx.Done():
			return
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
		}
	}
}

func (node *FabftNode) AsynchronousRoutine(ctx context.Context) {
	votesCount := 0
	qc := commons.QC{
		Signatures: make([]string, 0),
		VertexHash: 0,
		Round:      0,
		Sender:     node.NodeInfo.Address,
	}
	//node.asyncNewRound()
	for {
		select {
		case vote := <-node.VoteChannel:
			if node.verifyVote(vote, node.round) {
				qc.Signatures = append(qc.Signatures, vote.Signature)
				qc.VertexHash = vote.Hash
				votesCount++
			}
			if votesCount > len(node.peers)-node.f {
				node.broadcastQC(qc)
			}
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}

func (node *FabftNode) handleTimeout() {
	//node.log.Debug("buffer size:", node.buffer.Size(), "round", node.round, "node.dag.GetRound(node.round).Size()", node.dag.GetRound(node.round).Size())
	node.newRound()
	time.Sleep(300 * time.Millisecond)
	qc := node.checkLastRoundVotes(node.round)
	node.log.Println("qc generated for block:", qc.VertexHash)
	if qc != nil {
		go node.broadcastQC(*qc)
	}
	node.timer.Reset(node.timerTimeout)
}

func (node *FabftNode) generateVertex() *commons.Vertex {
	block := "transaction placeholder" + strconv.Itoa(rand.Int()) // placeholder, to be placed with real blocks with transactions
	bv := commons.BaseVertex{
		Source: node.NodeInfo.Address,
		Round:  node.round,
		Block:  block,
	}
	roundSet := node.dag.GetRound(node.round - 1)
	var hashPointers []commons.VHash
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
	vh, _ := hashstructure.Hash(v, nil)
	v.VertexHash = commons.VHash(vh)
	return v
}

func (node *FabftNode) verifyVertexMsg(vm *internal.BroadcastMessage[commons.Vertex, commons.Round]) {
	if node.verifyVertex(&vm.Message) {
		sig := node.signVertex(&vm.Message)
		node.vote(true, sig, &vm.Message)
	}
	_ = node.buffer.Put(vm.Message.VertexHash, &vm.Message, true)
}

func (node *FabftNode) verifyVertex(v *commons.Vertex) bool {
	// placeholder, need further implementation
	return true
}

func (node *FabftNode) broadcastVertex(v *commons.Vertex, r commons.Round) {
	for _, peer := range node.peers {
		if peer.NodeInfo.Address != node.NodeInfo.Address {
			clonedPeer := peer
			go func() {
				node.log.Println("message broadcastVertex to", clonedPeer.NodeInfo.Address, "v=", v, "r=", r)
				clonedPeer.VertexChannel <- internal.BroadcastMessage[commons.Vertex, commons.Round]{
					Message: *v,
					R:       r,
					P:       node.NodeInfo.Address,
				}
			}()
		}
	}
}

func (node *FabftNode) receiveQC(ctx context.Context) {
	for {
		select {
		case qc := <-node.QCChannel:
			go node.checkQC(qc)
		case <-ctx.Done():
			return
		}
	}
}

func (node *FabftNode) checkQC(qc commons.QC) {
	if !node.verifyQCSignatures(qc) {
		return
	}
	v, _ := node.buffer.Get(qc.VertexHash)
	node.checkAncestors(v)
}

func (node *FabftNode) checkAncestors(v *commons.Vertex) {
	lastRoundVertices := node.dag.GetRound(v.Round - 1).VertexMap()
	missing := false
	for _, ph := range v.PrevHashes {
		if _, ok := lastRoundVertices[ph]; !ok {
			_ = node.descendantsWaiting.Put(ph, make(chan commons.VHash), false) // initialize channel for the first time
			descendants, _ := node.descendantsWaiting.Get(ph)
			descendants <- v.VertexHash
			_ = node.missingAncestorsCount.Put(v.VertexHash, 0, false)
			node.missingAncestorsCount.IncrementValueInt64(v.VertexHash, 1) // increment count of the missing ancestor
			missing = true
		}
	}
	if !missing { // no prev vertices missing
		node.checkDescendantsReady(v)
	}
}

func (node *FabftNode) checkDescendantsReady(v *commons.Vertex) {
	_ = node.buffer.Delete(v.VertexHash)
	node.log.Println("vertex added to DAG:", v.VertexHash)
	node.addToDag(v)
	descendants, _ := node.descendantsWaiting.Get(v.VertexHash)
	descendantsReadyToAdd := make(chan *commons.Vertex, 1024)
	defer close(descendantsReadyToAdd)
	for d := range descendants {
		if node.missingAncestorsCount.IncrementValueInt64(d, -1) == 0 {
			_ = node.missingAncestorsCount.Delete(d)
			descendantsVertex, _ := node.buffer.Get(d)
			descendantsReadyToAdd <- descendantsVertex
		}
	}
	exitFlag := false
	for !exitFlag {
		select {
		case descendantReady := <-descendantsReadyToAdd:
			go node.checkDescendantsReady(descendantReady)
		default:
			exitFlag = true
		}
	}
}

func (node *FabftNode) vote(approved bool, signature string, v *commons.Vertex) {
	vSource, _ := node.peersMap.Get(v.Source)
	vSource.VoteChannel <- commons.Vote{
		Round:     v.Round,
		Approved:  approved,
		Signature: signature,
		Hash:      v.VertexHash,
	}
	node.log.Println("voted for vh=", v.VertexHash, "to: ", vSource.NodeInfo.Address)
}

func (node *FabftNode) signVertex(v *commons.Vertex) string {
	// placeholder
	return "signature"
}

func (node *FabftNode) checkLastRoundVotes(lastRound commons.Round) *commons.QC {
	if lastRound == 0 {
		return nil
	}
	node.log.Println("checking last round:", lastRound)
	qc := &commons.QC{
		Signatures: make([]string, 0),
		VertexHash: 0,
		Round:      lastRound,
		Sender:     node.NodeInfo.Address,
	}
	exitFlag := false
	for !exitFlag {
		select {
		case vote := <-node.VoteChannel:
			if node.verifyVote(vote, lastRound) {
				qc.Signatures = append(qc.Signatures, vote.Signature)
				qc.VertexHash = vote.Hash
			}
		default:
			exitFlag = true
		}
	}
	node.log.Println("valid votes received:", len(qc.Signatures))
	if len(qc.Signatures)+1 > node.n-node.f {
		v, _ := node.buffer.Get(qc.VertexHash)
		go node.checkAncestors(v)
		return qc
	}
	node.log.Println("timeout: not enough signatures for vertex on round:", lastRound)
	return nil
}

func (node *FabftNode) verifyQCSignatures(qc commons.QC) bool {
	return true // placeholder
}

func (node *FabftNode) verifyVote(vote commons.Vote, round commons.Round) bool {
	return vote.Round == round // placeholder
}

func (node *FabftNode) broadcastQC(qc commons.QC) {
	node.log.Debug("broadcasting QC for block:", qc.VertexHash, "in round", qc.Round)
	for _, peer := range node.peers {
		if peer.NodeInfo.Address != node.NodeInfo.Address {
			clonedPeer := peer
			/*go func() {
				clonedPeer.QCChannel <- qc
			}()*/
			clonedPeer.QCChannel <- qc
		}
	}
}

func (node *FabftNode) newRound() {
	node.round++ // new round
	node.log.Debug("round started:", node.round)
	b := node.generateVertex()
	node.log.Debug("round:", node.round, "block generated:", b.VertexHash, "prevHashes: ", b.PrevHashes)
	node.broadcastVertex(b, node.round) // broadcast block to all peers
	_ = node.buffer.Put(b.VertexHash, b, true)
}

func (node *FabftNode) addToDag(v *commons.Vertex) {
	node.dag.NewRoundIfNotExists(v.Round)
	node.dag.GetRound(v.Round).AddVertex(*v)
	fmt.Println("vertex added to dag, vh=", v.VertexHash, "round=", v.Round)
	for _, pr := range v.PrevHashes {
		copiedPr := pr
		go node.commitToFasterLedger(copiedPr, v.Round-1)
	}
}

func (node *FabftNode) commitToFasterLedger(vh commons.VHash, round commons.Round) {
	// needs to be implemented
	node.log.Println("block:", vh, "committed to the faster ledger at round:", round)
}

func (node *FabftNode) commitToSaferLedger(vh commons.VHash, round commons.Round) {

}
