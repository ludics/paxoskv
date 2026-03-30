package src

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
)

var benchPortOffset atomic.Int64

func nextPortOffset() int64 {
	return benchPortOffset.Add(10) - 10
}

func startBenchCluster(b *testing.B, n int) ([]int64, []*grpc.Server) {
	offset := nextPortOffset()
	acceptorIds := make([]int64, n)
	for i := 0; i < n; i++ {
		acceptorIds[i] = offset + int64(i+1)
	}
	servers := ServeAcceptors(acceptorIds)
	time.Sleep(100 * time.Millisecond)
	b.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
		for i := 0; i < n; i++ {
			os.RemoveAll(fmt.Sprintf("/tmp/paxoskv-leveldb-%d", acceptorIds[i]))
		}
	})
	return acceptorIds, servers
}

func startBenchClusterT(t *testing.T, n int) ([]int64, []*grpc.Server) {
	offset := nextPortOffset()
	acceptorIds := make([]int64, n)
	for i := 0; i < n; i++ {
		acceptorIds[i] = offset + int64(i+1)
	}
	servers := ServeAcceptors(acceptorIds)
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
		for i := 0; i < n; i++ {
			os.RemoveAll(fmt.Sprintf("/tmp/paxoskv-leveldb-%d", acceptorIds[i]))
		}
	})
	return acceptorIds, servers
}

// BenchmarkPaxosWrite measures single-proposer sequential write throughput
func BenchmarkPaxosWrite(b *testing.B) {
	for _, numAcceptors := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("acceptors=%d", numAcceptors), func(b *testing.B) {
			acceptorIds, _ := startBenchCluster(b, numAcceptors)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				p := &Proposer{
					InstanceId: &PaxosInstanceId{Key: "bench", Version: int64(i + 1)},
					Ballot:     &BallotNum{N: 1, ProposerId: 1},
				}
				val := p.RunPaxos(acceptorIds, &Value{Data: []byte("bench-value")})
				if val == nil {
					b.Fatal("RunPaxos returned nil")
				}
			}
		})
	}
}

// BenchmarkPaxosRead measures read throughput (Phase 1 only)
func BenchmarkPaxosRead(b *testing.B) {
	acceptorIds, _ := startBenchCluster(b, 3)

	// write one value first
	p := &Proposer{
		InstanceId: &PaxosInstanceId{Key: "read-bench", Version: 1},
		Ballot:     &BallotNum{N: 1, ProposerId: 1},
	}
	p.RunPaxos(acceptorIds, &Value{Data: []byte("read-me")})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &Proposer{
			InstanceId: &PaxosInstanceId{Key: "read-bench", Version: 1},
			Ballot:     &BallotNum{N: int64(i + 2), ProposerId: 1},
		}
		val := p.RunPaxos(acceptorIds, nil)
		if val == nil {
			b.Fatal("expected non-nil read")
		}
	}
}

// BenchmarkPaxosConcurrentWrites measures concurrent proposers writing different keys
func BenchmarkPaxosConcurrentWrites(b *testing.B) {
	for _, concurrency := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("c=%d", concurrency), func(b *testing.B) {
			acceptorIds, _ := startBenchCluster(b, 3)
			b.ResetTimer()

			var wg sync.WaitGroup
			var success atomic.Int64
			var fail atomic.Int64
			keysPerWorker := b.N / concurrency
			if keysPerWorker == 0 {
				keysPerWorker = 1
			}

			start := time.Now()
			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func(workerId int) {
					defer wg.Done()
					for k := 0; k < keysPerWorker; k++ {
						key := fmt.Sprintf("cw-%d-%d", workerId, k)
						p := &Proposer{
							InstanceId: &PaxosInstanceId{Key: key, Version: 1},
							Ballot:     &BallotNum{N: 1, ProposerId: int64(workerId + 1)},
						}
						val := p.RunPaxos(acceptorIds, &Value{Data: []byte(key)})
						if val != nil {
							success.Add(1)
						} else {
							fail.Add(1)
						}
					}
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(start)

			total := success.Load() + fail.Load()
			b.ReportMetric(float64(success.Load())/elapsed.Seconds(), "ops/s")
			b.ReportMetric(float64(fail.Load())/float64(total)*100, "fail%")
		})
	}
}

// BenchmarkPaxosCompetingProposers measures safety under contention on the same key
func BenchmarkPaxosCompetingProposers(b *testing.B) {
	for _, numProposers := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("proposers=%d", numProposers), func(b *testing.B) {
			acceptorIds, _ := startBenchCluster(b, 3)
			b.ResetTimer()

			var converged atomic.Int64

			start := time.Now()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(numProposers)
				key := fmt.Sprintf("compete-%d", i)
				valsMu := sync.Mutex{}
				vals := make([][]byte, 0, numProposers)

				for p := 0; p < numProposers; p++ {
					go func(proposerId int) {
						defer wg.Done()
						prop := &Proposer{
							InstanceId: &PaxosInstanceId{Key: key, Version: 1},
							Ballot:     &BallotNum{N: 1, ProposerId: int64(proposerId + 1)},
						}
						v := prop.RunPaxos(acceptorIds, &Value{
							Data: []byte(fmt.Sprintf("p%d", proposerId)),
						})
						if v != nil {
							valsMu.Lock()
							vals = append(vals, v.Data)
							valsMu.Unlock()
						}
					}(p)
				}
				wg.Wait()

				// check convergence: all results must be identical
				if len(vals) > 0 {
					first := string(vals[0])
					allSame := true
					for _, v := range vals[1:] {
						if string(v) != first {
							allSame = false
							break
						}
					}
					if allSame {
						converged.Add(1)
					}
				}
			}
			elapsed := time.Since(start)

			b.ReportMetric(float64(converged.Load())/elapsed.Seconds(), "converged/s")
			if int(converged.Load()) != b.N {
				b.Errorf("safety violation: only %d/%d converged to same value", converged.Load(), b.N)
			}
		})
	}
}

// TestStressHighConcurrency is a correctness stress test
func TestStressHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	acceptorIds, _ := startBenchClusterT(t, 5)
	concurrency := 50
	opsPerWorker := 100

	var wg sync.WaitGroup
	var success atomic.Int64
	var fail atomic.Int64

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for k := 0; k < opsPerWorker; k++ {
				key := fmt.Sprintf("stress-%d-%d", workerId, k)
				p := &Proposer{
					InstanceId: &PaxosInstanceId{Key: key, Version: 1},
					Ballot:     &BallotNum{N: 1, ProposerId: int64(workerId + 1)},
				}
				val := p.RunPaxos(acceptorIds, &Value{Data: []byte(key)})
				if val != nil {
					success.Add(1)
				} else {
					fail.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("stress test: %d ops in %v (%.0f ops/s), success=%d fail=%d",
		concurrency*opsPerWorker, elapsed,
		float64(concurrency*opsPerWorker)/elapsed.Seconds(),
		success.Load(), fail.Load())
}
