package strategy

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"ivanhawkes.dev/snowflake"
)

type reservePoolType []uint32
type strategyPoolListType []*Strategy

type StrategyPool struct {
	Pool      strategyPoolListType
	length    uint32
	poolIndex uint32
}

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
	// for i, value := range st.exhaustedQueue {
	// 	fmt.Printf("queue: %d\t%d\n", i, value)
	// }

	// Debug.
	fmt.Printf("RESERVE QUEUE: %d\n", len(st.reserveQueue))
	// for i, value := range st.reserveQueue {
	// 	fmt.Printf("queue: %d\t%d\n", i, value)
	// }

	// Debug.
	fmt.Printf("ID COUNT: %d\n", st.idCount)
	fmt.Printf("THREAD ID: %d\n", st.threadID)
	fmt.Printf("EXHAUSTION MAX: %d\n\n", st.exhaustedMax)
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

func NewStrategyPool(Epoch time.Time, EntryCount uint32, PoolStart uint32, ReservedPerPool uint32) (StrategyPool, error) {
	// Make a new pool and set the index and counter.
	sp := StrategyPool{}
	sp.poolIndex = EntryCount - 1
	sp.length = EntryCount

	for i := PoolStart; i < EntryCount; i++ {
		// Calculate the pool range.
		start := PoolStart + i*ReservedPerPool
		end := PoolStart + (i+1)*ReservedPerPool - 1

		// Add a strategy for each entry.
		st, err := NewStrategy(Epoch, start, end)
		if err != nil {
			fmt.Println("Failed to create a strategy pool.")
			return sp, err
		}
		sp.Pool = append(sp.Pool, st)
	}

	return sp, nil
}

func (sp *StrategyPool) Next() *Strategy {
	// Round robin.
	sp.poolIndex++
	if sp.poolIndex >= sp.length {
		sp.poolIndex = 0
	}

	return sp.Pool[sp.poolIndex]
}

func NewStrategy(Epoch time.Time, PoolMinimum uint32, PoolMaximum uint32) (*Strategy, error) {
	entries := PoolMaximum - PoolMinimum + 1

	st := new(Strategy{
		threadID:           0,
		wasExhausted:       false,
		exhaustedTimestamp: 0,
		reserveQueue:       make(reservePoolType, 0, entries),
		exhaustedQueue:     make(reservePoolType, 0, entries),
		exhaustedMax:       0,
		idCount:            0})

	// Create a pool of IDS to hold in reserve.
	for i := PoolMinimum; i <= PoolMaximum; i++ {
		st.reserveQueue = append(st.reserveQueue, PoolMinimum+i)
	}

	// Get an initial threadID.
	st.reserveQueue, st.threadID = dequeue(st.reserveQueue)

	// Make a new snowflake to use.
	sf, err := snowflake.New(Epoch, st.threadID)
	st.snowflake = sf

	return st, err
}

func (st *Strategy) NextID() uint64 {
	// Placing a mutex on this entire function will allow multiple
	// clients to share the same strategy. You can comment this out
	// if you're sure each execution thread / goroutine will have
	// it's own strategy instance.
	mutex := sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()

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
