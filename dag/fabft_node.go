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
	dag                    internal.Fabftdag
	round                  commons.Round
	buffer                 collections.Map[commons.VHash, *commons.Vertex]    // storing received blocks with no qc
	bufferWaitingAncestors collections.Map[commons.VHash, *commons.Vertex]    // buffer2, storing blocks with QC but missing prev blocks
	descendantsWaiting     collections.Map[commons.VHash, chan commons.VHash] // map hash of missing vertices to vertices that needs this vertex
	missingAncestorsCount  collections.IncrementableMap[commons.VHash]        // map vertices to the num of ancestors missing. add vertex to dag if this count hits 0
	// non-persistent info
	timer             *time.Timer
	timerTimeout      time.Duration
	networkAssumption commons.NetworkAssumption
	gpcShareSent      bool // if the threshold signature share of this round is already sent
	// channels
	VertexChannel        chan internal.BroadcastMessage[commons.Vertex, commons.Round]
	VoteChannel          chan commons.Vote
	AsyncNewRoundChannel chan bool
	BlockToPropose       chan commons.Block
	QCChannel            chan commons.QC
	SignChannel          chan commons.SignShare
	RequestHelpChannel   chan commons.HelpRequest
	LateVertexChannel    chan commons.Vertex
	signatures           []string
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
		signatures:             make([]string, 0),
		VoteChannel:            make(chan commons.Vote, 1024),
		SignChannel:            make(chan commons.SignShare, 1024),
		AsyncNewRoundChannel:   make(chan bool),
		RequestHelpChannel:     make(chan commons.HelpRequest, 1024),
		LateVertexChannel:      make(chan commons.Vertex, 1024),
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
		v.Block = "genesis block"
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
		go node.ReceiveGPCSignatures(ctx)
	}
}

func (node *FabftNode) ReceiveRoutine(ctx context.Context) {
	for {
		select {
		/*
			case rMsg := <-node.RBcastChannel:
				node.log.Debug("receive message from RBcastChannel", "p=", rMsg.P, "r=", rMsg.R, "m=", rMsg.Message)
				node.rDeliver(rMsg.Message, rMsg.R, rMsg.P)
		*/
		case qc := <-node.QCChannel:
			go node.checkQC(qc)
		case vertexMsg := <-node.VertexChannel:
			node.log.Debug("receive message from VertexChannel", "p=", vertexMsg.P, "r=", vertexMsg.R, "m=", vertexMsg.Message)
			go node.verifyVertexMsg(&vertexMsg)
		case helpRequest := <-node.RequestHelpChannel:
			go node.replyHelpRequest(helpRequest)
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
	node.newRound()
	node.gpcShareSent = false
	votesCount := 0
	qc := commons.QC{
		Signatures: make([]string, 0),
		VertexHash: 0,
		Round:      0,
		Sender:     node.NodeInfo.Address,
	}
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
		case <-node.AsyncNewRoundChannel:
			node.signatures = make([]string, 0) // flush previous signs
			node.newRound()
			node.gpcShareSent = false
			votesCount = 0
			qc = commons.QC{
				Signatures: make([]string, 0),
				VertexHash: 0,
				Round:      0,
				Sender:     node.NodeInfo.Address,
			}
		case <-ctx.Done():
			return
		}
	}
}

func (node *FabftNode) ReceiveGPCSignatures(ctx context.Context) {
	for {
		select {
		case sig := <-node.SignChannel:
			if sig.Round == node.round {
				node.signatures = append(node.signatures, sig.Sign)
			}
			if len(node.signatures) >= node.n-node.f {
				leader := node.SelectLeader(node.signatures)
				node.TryCommitLeaderAsync(leader, node.round-1)
			}
		}
	}
}

func (node *FabftNode) TryCommitLeaderAsync(leader commons.Address, round commons.Round) { // try committing leader's block on round round
	var leaderV commons.Vertex
	leaderVertexFound := false
	for _, v := range node.dag.GetRound(round).Entries() {
		if v.Source == leader {
			leaderV = v
			leaderVertexFound = true
			break
		}
	}
	if !leaderVertexFound {
		node.AsyncNewRoundChannel <- true
		return // leader block hasn't been added to dag, no vertices committed
	}
	refCount := 0
	for _, v := range node.dag.GetRound(round + 1).Entries() { // count references to leaderV on round+1
		for _, vh := range v.PrevHashes {
			if vh == leaderV.VertexHash {
				refCount++
				break
			}
		}
	}
	if refCount < node.n-node.f {
		node.AsyncNewRoundChannel <- true
		return // no enough references, no vertices committed
	}
	node.CommitPast(leaderV)
}

func (node *FabftNode) CommitPast(v commons.Vertex) {
	s := collections.NewStack[commons.Vertex]()
	s.Push(v)
	for s.Size() > 0 {
		curr := s.Pop()
		node.commitToSaferLedger(curr.VertexHash, curr.Round)
		for _, vh := range curr.PrevHashes {
			prevV := node.dag.GetRound(curr.Round - 1).GetByHash(vh)
			if !prevV.Delivered {
				s.Push(prevV)
			}
		}
	}
}

func (node *FabftNode) SelectLeader(signatures []string) commons.Address { // select leader for async block commit
	return node.peers[int(node.round)%len(node.peers)].NodeInfo.Address // need further GPC implementation
}

func (node *FabftNode) handleTimeout() {
	//node.log.Debug("buffer size:", node.buffer.Size(), "round", node.round, "node.dag.GetRound(node.round).Size()", node.dag.GetRound(node.round).Size())
	checkQCTimer := time.NewTimer(300 * time.Millisecond)
	node.timer.Reset(node.timerTimeout)
	node.newRound()
	<-checkQCTimer.C
	qc := node.checkLastRoundVotes(node.round)
	node.log.Println("qc generated for block:", qc.VertexHash)
	if qc != nil {
		go node.broadcastQC(*qc)
	}

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

func (node *FabftNode) checkQC(qc commons.QC) {
	if !node.verifyQCSignatures(qc) {
		return
	}
	v, _ := node.buffer.Get(qc.VertexHash)
	node.checkPrecursors(v)
}

func (node *FabftNode) checkPrecursors(v *commons.Vertex) {
	lastRoundVertices := node.dag.GetRound(v.Round - 1).VertexMap()
	missing := false
	for _, ph := range v.PrevHashes {
		if _, ok := lastRoundVertices[ph]; !ok {
			_ = node.descendantsWaiting.Put(ph, make(chan commons.VHash), false) // initialize channel for the first time
			descendants, _ := node.descendantsWaiting.Get(ph)
			descendants <- v.VertexHash
			node.callForHelp(ph, v.Round-1)
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
		go node.checkPrecursors(v)
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
	node.signatures = make([]string, 0)
	b := node.generateVertex()
	node.log.Debug("round:", node.round, "block generated:", b.VertexHash, "prevHashes: ", b.PrevHashes)
	node.broadcastVertex(b, node.round) // broadcast block to all peers
	_ = node.buffer.Put(b.VertexHash, b, true)
}

func (node *FabftNode) addToDag(v *commons.Vertex) {
	node.dag.NewRoundIfNotExists(v.Round)
	node.dag.GetRound(v.Round).AddVertex(*v)
	fmt.Println("vertex added to dag, vh=", v.VertexHash, "round=", v.Round)
	if node.networkAssumption == commons.PartiallySynchronous {
		for _, pr := range v.PrevHashes {
			copiedPr := pr
			go node.commitToFasterLedger(copiedPr, v.Round-1)
		}
	} else {
		if !node.gpcShareSent && node.dag.GetRound(v.Round).Size() > node.n-node.f {
			node.gpcShareSent = true
			node.sendGPCSignature()
		}
	}
}

func (node *FabftNode) commitToFasterLedger(vh commons.VHash, round commons.Round) {
	v := node.dag.GetRound(round).VertexMap()[vh]
	v.Delivered = true
	node.log.Println("block:", vh, "committed to the faster ledger at round:", round)

}

func (node *FabftNode) commitToSaferLedger(vh commons.VHash, round commons.Round) {
	v := node.dag.GetRound(round).VertexMap()[vh]
	v.Delivered = true
	node.log.Println("block:", vh, "committed to the safer ledger at round:", round)
}

func (node *FabftNode) callForHelp(vh commons.VHash, round commons.Round) {
	for _, peer := range node.peers {
		clonedPeer := peer
		clonedPeer.RequestHelpChannel <- commons.HelpRequest{VHash: vh, Round: round, Source: node.NodeInfo.Address}
	}
}

func (node *FabftNode) sendGPCSignature() {
	sig := "signature of node:" + string(node.NodeInfo.Address) + " round: " + strconv.FormatInt(int64(node.round), 10)
	for _, peer := range node.peers {
		clonedPeer := peer
		clonedPeer.SignChannel <- commons.SignShare{Sign: sig, Round: node.round}
	}
}

func (node *FabftNode) replyHelpRequest(vhr commons.HelpRequest) {
	peer, _ := node.peersMap.Get(vhr.Source)
	vMsg := internal.BroadcastMessage[commons.Vertex, commons.Round]{
		Message: node.dag.GetRound(vhr.Round).GetByHash(vhr.VHash),
		R:       vhr.Round,
		P:       node.NodeInfo.Address,
	}
	peer.VertexChannel <- vMsg
}
