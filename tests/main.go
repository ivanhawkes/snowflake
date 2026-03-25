package main

import (
	"fmt"
	"slices"
	"time"

	"ivanhawkes.dev/snowflake"
)

type threadIDMapType map[uint64]uint64
type reservePoolType []uint32

func enqueue(queue []uint32, element uint32) []uint32 {
	queue = append(queue, element)
	fmt.Printf("Enqueue: %d \n", element)

	return queue
}

func dequeue(queue []uint32) ([]uint32, uint32) {
	element := queue[0]
	fmt.Printf("Dequeue: %d \n", element)

	return queue[1:], element
}

func main() {
	var threadID uint32 = 0
	var wasExhausted bool = false
	var exhaustedTimestamp uint64 = 0
	var reserveQueue = make(reservePoolType, 0, 64)
	var exhaustedQueue = make(reservePoolType, 0, 64)

	// By inserting the values into a map we can easily check for duplicates.
	m := make(threadIDMapType)

	// Create a pool of IDS to hold in reserve.
	for range 64 {
		reserveQueue = append(reserveQueue, threadID)
		threadID++
	}

	// Get an initial threadID.
	reserveQueue, threadID = dequeue(reserveQueue)

	// Track all the IDs that have been used from the pool.
	epoch := time.Time{}
	sf, err := snowflake.New(epoch, threadID)
	if err != nil {
		fmt.Println("Failed to create a snowflake generator.")
	}

	fmt.Printf("Epoch: %v\n", epoch)

	for i := 0; i < 100; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		time.Sleep(75000)

		// Get the next ID available.
		id, isExhausted := sf.NextID()

		// Recover from exhaustion if possible.
		if wasExhausted == true {
			if snowflake.TimeStamp(id) > exhaustedTimestamp {
				fmt.Println("RECOVERY")

				// All the exhausted ThreadIDs can return to the pool we use.
				if len(exhaustedQueue) > 0 {
					fmt.Println("DEQUEUE")

					// Copy the exhausted list back to our pool.
					for _, value := range exhaustedQueue {
						fmt.Printf("Copy: %d\n", value)
						reserveQueue = enqueue(reserveQueue, value)
					}
					slices.Sort(reserveQueue)

					// Empty the exhausted LIFO.
					exhaustedQueue = exhaustedQueue[0:0]

					// Take a new threadID from the top of the queue.
					// reserveQueue, threadID = dequeue(reserveQueue)
					// sf.ResetID(threadID)
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
			// We requested a new ID before the timestamp rolled over. We need a
			// new ThreadID to avoid conflicts.
			reserveQueue, threadID = dequeue(reserveQueue)
			previousID := sf.ResetID(threadID)
			exhaustedTimestamp = snowflake.TimeStamp(id)
			fmt.Printf("\nEXHAUSTED: i= %d, previousID=%d, threadID = %d\n", i, previousID, threadID)
			isExhausted = false
			wasExhausted = true

			// Need to add any ID that's not the original one to a pool
			// of exhausted IDs for later recovery.
			fmt.Printf("Queue Exhausted: %d\n", previousID)
			exhaustedQueue = enqueue(exhaustedQueue, previousID)
		} else {

		}

		/*if i%8 == 0 || i%8 == 7 || isExhausted*/
		{
			fmt.Printf("\ni: %d,\tmap size: %d\n", i, len(m))
			fmt.Printf("NextId: %d\tTimeStamp: %d\n", id, snowflake.TimeStamp(id))
			fmt.Printf("ThreadID: %d\tSequenceNumber: %d\n", snowflake.ThreadID(id), snowflake.SequenceNumber(id))
		}
	}

	// Debug.
	fmt.Printf("THREAD ID: %d\n", threadID)

	// Debug.
	slices.Sort(exhaustedQueue)
	fmt.Printf("EXHAUSTED QUEUE: %d\n", len(exhaustedQueue))
	for i, value := range exhaustedQueue {
		fmt.Printf("queue: %d\t%d\n", i, value)
	}

	// Debug.
	slices.Sort(reserveQueue)
	fmt.Printf("RESERVE QUEUE: %d\n", len(reserveQueue))
	for i, value := range reserveQueue {
		if i < 10 {
			fmt.Printf("queue: %d\t%d\n", i, value)
		}
	}

}
