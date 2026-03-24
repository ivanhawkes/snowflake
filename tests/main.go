package main

import (
	"fmt"
	"time"

	"ivanhawkes.dev/snowflake"
)

func enqueue(queue []uint32, element uint32) []uint32 {
	queue = append(queue, element)
	fmt.Printf("Enqueued: %d \n", element)

	return queue
}

func dequeueAll(queue []uint32) []uint32 {
	for i := len(queue) - 1; i >= 0; i-- {
		threadID := queue[i]
		fmt.Printf("Dequeue: %d\n", threadID)
	}

	return queue[0:0]
}

func main() {
	var threadID uint32 = 0
	var wasExhausted bool = false
	var exhaustedTimestamp uint64 = 0
	var exhaustedLIFO []uint32

	// By inserting the values into a map we can easily check for duplicates.
	m := make(map[uint64]uint64)

	// Create a pool of IDS to hold in reserve.
	reservePool := make(map[uint64]uint64)
	for i := 0; i < 64; i++ {
		reservePool[uint64(threadID)] = uint64(threadID)
		threadID++
	}

	// Keep a copy of the original ID so we can return to that value
	// when the rush is over.
	originalID := threadID

	// Track all the IDs that have been used from the pool.
	epoch := time.Time{}
	sf, err := snowflake.New(epoch, threadID)
	if err != nil {
		fmt.Println("Failed to create a snowflake generator.")
	}

	fmt.Printf("Epoch: %v\n", epoch)

	for i := 0; i < 20; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		time.Sleep(1000000)

		// Get the next ID available.
		id, isExhausted := sf.NextID()

		// Recover from exhaustion if possible.
		if wasExhausted == true {
			if snowflake.TimeStamp(id) > exhaustedTimestamp {
				fmt.Println("RECOVERY")

				// All the exhausted ThreadIDs can return to the pool we use.
				if len(exhaustedLIFO) > 0 {
					fmt.Println("DEQUEUE")
					exhaustedLIFO = dequeueAll(exhaustedLIFO)
					sf.ResetID(originalID)
				}

				// Mark this phase as completed.
				wasExhausted = false
			}
		}

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
			previousID := sf.ResetID(threadID)
			exhaustedTimestamp = snowflake.TimeStamp(id)
			fmt.Printf("\nEXHAUSTED: i= %d, oldID=%d, threadID = %d", i, originalID, threadID)
			isExhausted = false
			wasExhausted = true

			// Need to add any ID that's not the original one to a pool
			// of exhausted IDs for later recovery.
			if previousID != originalID {
				exhaustedLIFO = enqueue(exhaustedLIFO, previousID)
			}
			// }
		} else {

		}

		/*if i%8 == 0 || i%8 == 7 || isExhausted*/
		{
			fmt.Printf("\ni: %d,\tmap size: %d\n", i, len(m))
			fmt.Printf("NextId: %d\tTimeStamp: %d\n", id, snowflake.TimeStamp(id))
			fmt.Printf("ThreadID: %d\tSequenceNumber: %d\n", snowflake.ThreadID(id), snowflake.SequenceNumber(id))
		}
	}
}
