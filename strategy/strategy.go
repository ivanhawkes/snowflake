package strategy

import (
	"fmt"
	"log"
	"slices"
	"time"

	"ivanhawkes.dev/snowflake"
)

type reservePoolType []uint32

type Strategy struct {
	snowflake          *snowflake.Snowflake
	threadID           uint32
	reserveQueue       reservePoolType
	exhaustedQueue     reservePoolType
	exhaustedMax       int
	wasExhausted       bool
	exhaustedTimestamp uint64
	idCount            uint64
}

type Statistics struct {
	ThreadID             uint32
	ReserveQueueLength   int
	ExhaustedQueueLength int
	ExhaustedMax         int
	IdCount              uint64
}

func (st *Strategy) GetStatistics() Statistics {
	stats := Statistics{
		ThreadID:             st.threadID,
		ReserveQueueLength:   len(st.reserveQueue),
		ExhaustedQueueLength: len(st.exhaustedQueue),
		ExhaustedMax:         st.exhaustedMax,
		IdCount:              st.idCount}

	return stats
}

func (st *Strategy) PrintStatistics() {
	// Debug.
	fmt.Printf("EXHAUSTED QUEUE: %d\n", len(st.exhaustedQueue))
	for i, value := range st.exhaustedQueue {
		fmt.Printf("queue: %d\t%d\n", i, value)
	}

	// Debug.
	fmt.Printf("RESERVE QUEUE: %d\n", len(st.reserveQueue))
	for i, value := range st.reserveQueue {
		fmt.Printf("queue: %d\t%d\n", i, value)
	}

	// Debug.
	fmt.Printf("\nID COUNT: %d\n", st.idCount)
	fmt.Printf("THREAD ID: %d\n", st.threadID)
	fmt.Printf("EXHAUSTION MAX: %d\n", st.exhaustedMax)
}

func enqueue(queue []uint32, element uint32) []uint32 {
	queue = append(queue, element)

	return queue
}

func dequeue(queue []uint32) ([]uint32, uint32) {
	if len(queue) == 0 {
		log.Fatal("--- RESERVE THREAD IDs COMPLETELY EXHAUSTED ---")
	}

	element := queue[0]

	return queue[1:], element
}

func NewStrategy(Epoch time.Time, poolMinimum int, poolMaximum int) (*Strategy, error) {
	st := new(Strategy{
		threadID:           0,
		wasExhausted:       false,
		exhaustedTimestamp: 0,
		reserveQueue:       make(reservePoolType, 0, 64),
		exhaustedQueue:     make(reservePoolType, 0, 64),
		exhaustedMax:       0,
		idCount:            0})

	// Create a pool of IDS to hold in reserve.
	for i := poolMinimum; i <= poolMaximum; i++ {
		st.reserveQueue = append(st.reserveQueue, st.threadID)
		st.threadID++
	}

	// Get an initial threadID.
	st.reserveQueue, st.threadID = dequeue(st.reserveQueue)

	// Make a new snowflake to use.
	sf, err := snowflake.New(Epoch, st.threadID)
	st.snowflake = sf

	return st, err
}

func (st *Strategy) NextID() uint64 {
	// DEBUG: waste some time so we can test the timestamp bucketing.
	//time.Sleep(1000)

	// Get the next ID available.
	id, isExhausted := st.snowflake.NextID()
	st.idCount++

	// Recover from exhaustion if possible.
	if st.wasExhausted == true {
		if snowflake.TimeStamp(id) > st.exhaustedTimestamp {
			// All the exhausted ThreadIDs can return to the pool we use.
			if len(st.exhaustedQueue) > 0 {
				// Copy the exhausted list back to our pool.
				for _, value := range st.exhaustedQueue {
					st.reserveQueue = enqueue(st.reserveQueue, value)
				}
				slices.Sort(st.reserveQueue)

				// Empty the exhausted LIFO.
				st.exhaustedQueue = st.exhaustedQueue[0:0]
			}

			// Mark this phase as completed.
			st.wasExhausted = false
		}
	}

	// Check if the range of the sequence is exhausted.
	if isExhausted {
		// We requested a new ID before the timestamp rolled over. We need a
		// new ThreadID to avoid conflicts.
		st.reserveQueue, st.threadID = dequeue(st.reserveQueue)
		previousID := st.snowflake.ResetID(st.threadID)
		st.exhaustedTimestamp = snowflake.TimeStamp(id)
		isExhausted = false
		st.wasExhausted = true

		// Track the highest amount of exhausted values.
		if st.exhaustedMax < len(st.exhaustedQueue) {
			st.exhaustedMax = len(st.exhaustedQueue)
		}

		// Need to add any ID that's not the original one to a pool
		// of exhausted IDs for later recovery.
		st.exhaustedQueue = enqueue(st.exhaustedQueue, previousID)
	}

	return id
}
