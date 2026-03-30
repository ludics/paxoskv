package src

import (
	context "context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

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

type Version struct {
	mu       sync.Mutex
	acceptor Acceptor
}

type Versions map[int64]*Version

type KVServer struct {
	mu      sync.Mutex
	Storage map[string]Versions
}

// mustEmbedUnimplementedPaxosKVServer implements [PaxosKVServer].
func (s *KVServer) mustEmbedUnimplementedPaxosKVServer() {
	panic("unimplemented")
}

func (s *KVServer) getLockedVersion(id *PaxosInstanceId) *Version {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.Key
	ver := id.Version
	rec, found := s.Storage[key]
	if !found {
		rec = Versions{}
		s.Storage[key] = rec
	}
	v, found := rec[ver]
	if !found {
		rec[ver] = &Version{
			acceptor: Acceptor{
				LastBallot: &BallotNum{},
				VBallot:    &BallotNum{},
				Val:        &Value{},
			},
		}
		v = rec[ver]
	}
	v.mu.Lock()
	return v
}

func (s *KVServer) Prepare(c context.Context, r *Proposer) (*Acceptor, error) {
	v := s.getLockedVersion(r.InstanceId)
	defer v.mu.Unlock()

	if r.Ballot.GE(v.acceptor.LastBallot) {
		v.acceptor.LastBallot = r.Ballot
	}

	reply := Acceptor{
		LastBallot: &*v.acceptor.LastBallot,
		VBallot:    &*v.acceptor.VBallot,
		Val:        &*v.acceptor.Val,
	}

	return &reply, nil
}

func (s *KVServer) Accept(c context.Context, r *Proposer) (*Acceptor, error) {
	v := s.getLockedVersion(r.InstanceId)
	defer v.mu.Unlock()

	reply := Acceptor{
		LastBallot: &*v.acceptor.LastBallot,
	}

	if r.Ballot.GE(v.acceptor.LastBallot) {
		v.acceptor.LastBallot = r.Ballot
		v.acceptor.Val = r.Val
		v.acceptor.VBallot = r.Ballot
	}

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

		s := grpc.NewServer()
		RegisterPaxosKVServer(s, &KVServer{Storage: map[string]Versions{}})
		reflection.Register(s)
		log.Printf("Acceptor %d listening at %v", aid, lis.Addr())

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
			// 请求被拒绝了
			if reply.LastBallot.GE(higherBallot) {
				higherBallot = reply.LastBallot
			}
			continue
		}

		// 只有当 acceptor 确实投过票（VBallot 不为零值）时，才记录已投票的值
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
			// rejected
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
			// 读操作，只执行 Phase 1
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
