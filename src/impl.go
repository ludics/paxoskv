package src

import (
	context "context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func (a *BallotNum) GE(other *BallotNum) bool {
	if a.N > other.N {
		return true
	}
	if a.N < other.N {
		return false
	}
	return a.ProposerId >= other.ProposerId
}

// acceptorState is the JSON-serializable form of an Acceptor, persisted to LevelDB.
type acceptorState struct {
	LastBallotN          int64  `json:"last_ballot_n"`
	LastBallotProposerId int64  `json:"last_ballot_proposer_id"`
	VBallotN             int64  `json:"v_ballot_n"`
	VBallotProposerId    int64  `json:"v_ballot_proposer_id"`
	ValData              []byte `json:"val_data"`
	HasVal               bool   `json:"has_val"`
}

// Per-key/version lock to serialize Paxos phases on the same instance.
type versionLock struct {
	mu sync.Mutex
}

type KVServer struct {
	mu  sync.Mutex // protects versionLocks map
	db  *leveldb.DB
	locks map[string]*versionLock // key:ver -> lock
}

func (s *KVServer) mustEmbedUnimplementedPaxosKVServer() {
	panic("unimplemented")
}

// dbKey builds a LevelDB key from a PaxosInstanceId: "paxos:<key>:<version>"
func dbKey(id *PaxosInstanceId) string {
	return fmt.Sprintf("paxos:%s:%d", id.Key, id.Version)
}

func (s *KVServer) getLock(id *PaxosInstanceId) *versionLock {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := dbKey(id)
	lk, ok := s.locks[k]
	if !ok {
		lk = &versionLock{}
		s.locks[k] = lk
	}
	return lk
}

func (s *KVServer) loadState(id *PaxosInstanceId) acceptorState {
	raw, err := s.db.Get([]byte(dbKey(id)), nil)
	if err != nil {
		return acceptorState{}
	}
	var st acceptorState
	if err := json.Unmarshal(raw, &st); err != nil {
		return acceptorState{}
	}
	return st
}

func (s *KVServer) saveState(id *PaxosInstanceId, st acceptorState) {
	raw, err := json.Marshal(st)
	if err != nil {
		log.Printf("marshal state error: %v", err)
		return
	}
	if err := s.db.Put([]byte(dbKey(id)), raw, nil); err != nil {
		log.Printf("leveldb put error: %v", err)
	}
}

func stateToAcceptor(st acceptorState) Acceptor {
	lastBallot := &BallotNum{N: st.LastBallotN, ProposerId: st.LastBallotProposerId}
	vBallot := &BallotNum{N: st.VBallotN, ProposerId: st.VBallotProposerId}
	val := &Value{}
	if st.HasVal {
		val.Data = st.ValData
	}
	return Acceptor{
		LastBallot: lastBallot,
		VBallot:    vBallot,
		Val:        val,
	}
}

func (s *KVServer) Prepare(c context.Context, r *Proposer) (*Acceptor, error) {
	lk := s.getLock(r.InstanceId)
	lk.mu.Lock()
	defer lk.mu.Unlock()

	st := s.loadState(r.InstanceId)
	lastBallot := &BallotNum{N: st.LastBallotN, ProposerId: st.LastBallotProposerId}

	if r.Ballot.GE(lastBallot) {
		st.LastBallotN = r.Ballot.N
		st.LastBallotProposerId = r.Ballot.ProposerId
		s.saveState(r.InstanceId, st)
	}

	reply := stateToAcceptor(st)
	return &reply, nil
}

func (s *KVServer) Accept(c context.Context, r *Proposer) (*Acceptor, error) {
	lk := s.getLock(r.InstanceId)
	lk.mu.Lock()
	defer lk.mu.Unlock()

	st := s.loadState(r.InstanceId)
	lastBallot := &BallotNum{N: st.LastBallotN, ProposerId: st.LastBallotProposerId}

	replyLast := &BallotNum{N: st.LastBallotN, ProposerId: st.LastBallotProposerId}

	if r.Ballot.GE(lastBallot) {
		st.LastBallotN = r.Ballot.N
		st.LastBallotProposerId = r.Ballot.ProposerId
		st.VBallotN = r.Ballot.N
		st.VBallotProposerId = r.Ballot.ProposerId
		if r.Val != nil {
			st.ValData = r.Val.Data
			st.HasVal = true
		}
		s.saveState(r.InstanceId, st)
	}

	reply := Acceptor{LastBallot: replyLast}
	return &reply, nil
}

func ServeAcceptors(acceptorIds []int64) []*grpc.Server {
	servers := []*grpc.Server{}

	for _, aid := range acceptorIds {
		addr := fmt.Sprintf(":%d", 1024+int(aid))
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Each acceptor gets its own LevelDB database
		dbPath := fmt.Sprintf("/tmp/paxoskv-leveldb-%d", aid)
		db, err := leveldb.OpenFile(dbPath, nil)
		if err != nil {
			log.Fatalf("failed to open leveldb: %v", err)
		}

		s := grpc.NewServer()
		RegisterPaxosKVServer(s, &KVServer{
			db:    db,
			locks: map[string]*versionLock{},
		})
		reflection.Register(s)
		log.Printf("Acceptor %d listening at %v (db: %s)", aid, lis.Addr(), dbPath)

		servers = append(servers, s)
		go s.Serve(lis)
	}
	return servers
}

func (p *Proposer) Phase1(acceptorIds []int64, quorum int) (*Value, *BallotNum, error) {
	replies := p.rpcToAll(acceptorIds, "Prepare")

	ok := 0
	higherBallot := p.Ballot
	maxVoted := &Acceptor{VBallot: &BallotNum{}}
	hasVoted := false

	for _, reply := range replies {
		if !p.Ballot.GE(reply.LastBallot) {
			if reply.LastBallot.GE(higherBallot) {
				higherBallot = reply.LastBallot
			}
			continue
		}

		if reply.VBallot != nil && (reply.VBallot.N > 0 || reply.VBallot.ProposerId > 0) {
			if reply.VBallot.GE(maxVoted.VBallot) {
				maxVoted = reply
				hasVoted = true
			}
		}

		ok += 1
		if ok >= quorum {
			if hasVoted {
				return maxVoted.Val, nil, nil
			}
			return nil, nil, nil
		}
	}
	return nil, higherBallot, fmt.Errorf("Phase 1 failed, only %d replies received", ok)
}

func (p *Proposer) Phase2(acceptorIds []int64, quorum int) (*BallotNum, error) {
	replies := p.rpcToAll(acceptorIds, "Accept")

	ok := 0
	higherBallot := p.Ballot

	for _, reply := range replies {
		if !p.Ballot.GE(reply.LastBallot) {
			if reply.LastBallot.GE(higherBallot) {
				higherBallot = reply.LastBallot
			}
			continue
		}

		ok += 1
		if ok >= quorum {
			return nil, nil
		}
	}
	return higherBallot, fmt.Errorf("Phase 2 failed, only %d replies received", ok)
}

func (p *Proposer) rpcToAll(acceptorIds []int64, action string) []*Acceptor {
	replies := []*Acceptor{}

	for _, aid := range acceptorIds {
		address := fmt.Sprintf("127.0.0.1:%d", 1024+int(aid))
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("failed to dial: %v", err)
		}
		defer conn.Close()

		c := NewPaxosKVClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var reply *Acceptor
		if action == "Prepare" {
			reply, err = c.Prepare(ctx, p)
		} else if action == "Accept" {
			reply, err = c.Accept(ctx, p)
		}
		if err != nil {
			log.Printf("failed to %s: %v", action, err)
			continue
		}
		replies = append(replies, reply)
	}
	return replies
}

func (p *Proposer) RunPaxos(acceptorIds []int64, val *Value) *Value {
	quorum := len(acceptorIds)/2 + 1

	for {
		p.Val = val

		maxVotedVal, higherBallot, err := p.Phase1(acceptorIds, quorum)
		if err != nil {
			p.Ballot.N = higherBallot.N + 1
			continue
		}

		if maxVotedVal != nil {
			p.Val = maxVotedVal
		}

		if p.Val == nil {
			return nil
		}

		higherBallot, err = p.Phase2(acceptorIds, quorum)

		if err != nil {
			p.Ballot.N = higherBallot.N + 1
			continue
		}

		return p.Val
	}
}
