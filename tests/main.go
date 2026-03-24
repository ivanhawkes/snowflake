package main

import (
	"fmt"
	"time"

	"ivanhawkes.dev/snowflake"
)

func main() {
	var threadID uint32 = 0
	originalID := threadID
	// var exhaustedTimestamp uint64 = 0

	// By inserting the values into a map we can easily check for duplicates.
	m := make(map[uint64]uint64)

	epoch := time.Time{}
	sf, err := snowflake.New(epoch, threadID)
	if err != nil {
		fmt.Println("Failed to create a snowflake generator.")
	}

	fmt.Printf("Epoch: %v\n", epoch)

	for i := 0; i < 16; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		//time.Sleep(1000000)

		// Get the next ID available.
		id, isExhausted := sf.NextID()

		// Store it in a map for debugging purposes.
		//m[id] = id
		_, exists := m[id]
		if !exists {
			m[id] = id
		} else {
			fmt.Printf("--- CONFLICT ON %d, %d ---", id, m[id])
		}

		// Check if the range of the sequence is exhausted.
		if isExhausted {
			// Has our clock ticked us on to a new timestamp?
			// if exhaustedTimestamp < snowflake.TimeStamp(id) {
			// 	// The timestamp rolled over, so there is no need to take any action.
			// 	exhaustedTimestamp = 0
			// 	isExhausted = false
			// } else {
			// We requested a new ID before the timestamp rolled over. We need a
			// new ThreadID to avoid conflicts.
			threadID++
			originalID = sf.ResetID(threadID)
			// exhaustedTimestamp = snowflake.TimeStamp(id)
			// }
		}

		/*if i%8 == 0 || i%8 == 7 || isExhausted*/
		{
			fmt.Printf("\ni: %d,\tmap size: %d\n", i, len(m))
			fmt.Printf("NextId: %d\tTimeStamp: %d\n", id, snowflake.TimeStamp(id))
			fmt.Printf("ThreadID: %d\tSequenceNumber: %d\n", snowflake.ThreadID(id), snowflake.SequenceNumber(id))
		}

		// DEBUG: notify them.
		if isExhausted {
			fmt.Printf("EXHAUSTED: i= %d, oldID=%d, threadID = %d\n\n", i, originalID, threadID)
		}
	}
}
