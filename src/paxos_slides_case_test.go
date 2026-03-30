package src

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func startTestCluster(t *testing.T, n int) ([]int64, []*grpc.Server) {
	acceptorIds := make([]int64, n)
	for i := 0; i < n; i++ {
		acceptorIds[i] = int64(i + 1)
	}
	servers := ServeAcceptors(acceptorIds)
	time.Sleep(50 * time.Millisecond) // wait for servers to start
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
		for i := 0; i < n; i++ {
			os.RemoveAll(fmt.Sprintf("/tmp/paxoskv-leveldb-%d", i+1))
		}
	})
	return acceptorIds, servers
}

func newProposer(key string, ver int64, proposerId int64) *Proposer {
	return &Proposer{
		InstanceId: &PaxosInstanceId{
			Key:     key,
			Version: ver,
		},
		Ballot: &BallotNum{
			N:          1,
			ProposerId: proposerId,
		},
	}
}

// Case 1: 单个 Proposer 写入并读取
func TestCase1SingleProposer(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	prop := newProposer("key1", 1, 1)
	val := prop.RunPaxos(acceptorIds, &Value{Data: []byte("hello")})
	ta.NotNil(val)
	ta.Equal([]byte("hello"), val.Data)

	// 读取刚写入的值
	prop2 := newProposer("key1", 1, 1)
	got := prop2.RunPaxos(acceptorIds, nil)
	ta.NotNil(got)
	ta.Equal([]byte("hello"), got.Data)
}

// Case 2: 多个版本的写入和读取
func TestCase2MultipleVersions(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	// 写入 v1
	p1 := newProposer("user", 1, 1)
	v1 := p1.RunPaxos(acceptorIds, &Value{Data: []byte("alice")})
	ta.Equal([]byte("alice"), v1.Data)

	// 写入 v2
	p2 := newProposer("user", 2, 1)
	v2 := p2.RunPaxos(acceptorIds, &Value{Data: []byte("bob")})
	ta.Equal([]byte("bob"), v2.Data)

	// 读取 v1 仍然是 alice
	got1 := newProposer("user", 1, 1).RunPaxos(acceptorIds, nil)
	ta.Equal([]byte("alice"), got1.Data)

	// 读取 v2 是 bob
	got2 := newProposer("user", 2, 1).RunPaxos(acceptorIds, nil)
	ta.Equal([]byte("bob"), got2.Data)
}

// Case 3: 两个 Proposer 竞争同一个 key/version，值应该被收敛到一个
func TestCase3ConcurrentProposers(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	done := make(chan *Value, 2)

	go func() {
		p := newProposer("shared", 1, 1)
		v := p.RunPaxos(acceptorIds, &Value{Data: []byte("from-proposer-1")})
		done <- v
	}()
	go func() {
		p := newProposer("shared", 1, 2)
		v := p.RunPaxos(acceptorIds, &Value{Data: []byte("from-proposer-2")})
		done <- v
	}()

	v1 := <-done
	v2 := <-done

	ta.NotNil(v1)
	ta.NotNil(v2)
	// 两个 Proposer 应该收敛到同一个值
	ta.Equal(v1.Data, v2.Data)

	// 读取也应该是同一个值
	got := newProposer("shared", 1, 3).RunPaxos(acceptorIds, nil)
	ta.NotNil(got)
	ta.Equal(v1.Data, got.Data)
}

// Case 4: 不同 key 互不干扰
func TestCase4DifferentKeys(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	p1 := newProposer("keyA", 1, 1)
	v1 := p1.RunPaxos(acceptorIds, &Value{Data: []byte("valA")})
	ta.Equal([]byte("valA"), v1.Data)

	p2 := newProposer("keyB", 1, 1)
	v2 := p2.RunPaxos(acceptorIds, &Value{Data: []byte("valB")})
	ta.Equal([]byte("valB"), v2.Data)

	// 互相不影响
	gotA := newProposer("keyA", 1, 1).RunPaxos(acceptorIds, nil)
	ta.Equal([]byte("valA"), gotA.Data)

	gotB := newProposer("keyB", 1, 1).RunPaxos(acceptorIds, nil)
	ta.Equal([]byte("valB"), gotB.Data)
}

// Case 5: 读取不存在的 key 返回 nil
func TestCase5ReadNonExistent(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	p := newProposer("nokey", 1, 1)
	v := p.RunPaxos(acceptorIds, nil)
	ta.Nil(v) // 没有写过，读取应该返回 nil
}

// Case 6: Paxos 语义——高 ballot proposer 会继承已 accepted 的值
// 这是 Paxos 的核心安全性：一旦值被 chosen，后续任何 ballot 都必须保留该值
func TestCase6HigherBallotInheritsAccepted(t *testing.T) {
	ta := require.New(t)
	acceptorIds, _ := startTestCluster(t, 3)

	// ballot N=1 写入 "first"
	p1 := newProposer("ow", 1, 1)
	v1 := p1.RunPaxos(acceptorIds, &Value{Data: []byte("first")})
	ta.Equal([]byte("first"), v1.Data)

	// ballot N=2 尝试写入 "second"，但 Paxos 会继承已 chosen 的值 "first"
	p2 := &Proposer{
		InstanceId: &PaxosInstanceId{Key: "ow", Version: 1},
		Ballot:     &BallotNum{N: 2, ProposerId: 2},
	}
	v2 := p2.RunPaxos(acceptorIds, &Value{Data: []byte("second")})
	ta.Equal([]byte("first"), v2.Data, "higher ballot should inherit already-chosen value")

	// 读取仍然是 "first"
	got := newProposer("ow", 1, 1).RunPaxos(acceptorIds, nil)
	ta.Equal([]byte("first"), got.Data)
}
