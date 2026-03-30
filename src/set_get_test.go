package src

import "fmt"

func Example_setAndGetByKeyVer() {
	acceptorIds := []int64{1, 2, 3}

	servers := ServeAcceptors(acceptorIds)

	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	{
		prop := Proposer{
			InstanceId: &PaxosInstanceId{
				Key:     "testKey",
				Version: 1,
			},
			Ballot: &BallotNum{
				N:          1,
				ProposerId: 2,
			},
		}
		v := prop.RunPaxos(acceptorIds, &Value{
			Data: []byte("testValue"),
		})

		fmt.Printf("Set value: %s\n", string(v.Data))
	}

	{
		// get value
		prop := Proposer{
			InstanceId: &PaxosInstanceId{
				Key:     "testKey",
				Version: 1,
			},
			Ballot: &BallotNum{
				N:          1,
				ProposerId: 2,
			},
		}
		v := prop.RunPaxos(acceptorIds, nil)

		fmt.Printf("Get value: %s\n", string(v.Data))
	}

	{
		// set ver 2
		prop := Proposer{
			InstanceId: &PaxosInstanceId{
				Key:     "testKey",
				Version: 2,
			},
			Ballot: &BallotNum{
				N:          1,
				ProposerId: 2,
			},
		}
		v := prop.RunPaxos(acceptorIds, &Value{
			Data: []byte("testValue2"),
		})

		fmt.Printf("Set value: %s\n", string(v.Data))
	}

	{
		// get ver 2
		prop := Proposer{
			InstanceId: &PaxosInstanceId{
				Key:     "testKey",
				Version: 2,
			},
			Ballot: &BallotNum{
				N:          1,
				ProposerId: 2,
			},
		}
		v := prop.RunPaxos(acceptorIds, nil)

		fmt.Printf("Get value: %s\n", string(v.Data))
	}

	// Output:
	// Set value: testValue
	// Get value: testValue
	// Set value: testValue2
	// Get value: testValue2
}
