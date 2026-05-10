package strategy

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ivanhawkes/snowflake/snowflake"
	"go.uber.org/zap"
)

type reservePoolType []uint32
type poolListType []*Strategy

type Pool struct {
	Pool      poolListType
	length    uint32
	poolIndex uint32
	mutex     sync.Mutex
}

type Strategy struct {
	snowmachine        *snowflake.SnowMachine
	threadID           uint32
	reserveQueue       reservePoolType
	exhaustedQueue     reservePoolType
	exhaustedMax       int
	wasExhausted       bool
	exhaustedTimestamp snowflake.Flake
	idCount            uint64
	zl                 *zap.Logger
	mutex              sync.Mutex
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

// Output some of the statistics to the console.
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

func dequeue(queue []uint32, zl *zap.Logger) ([]uint32, uint32) {
	if len(queue) == 0 {
		zl.Fatal("Reserve thread IDs are completely exhausted.")
	}

	element := queue[0]

	return queue[1:], element
}

// Create a new pool of strategies which can be used to allocate IDs.
func NewStrategyPool(Epoch time.Time, EntryCount uint32, PoolStart uint32, ReservedPerPool uint32, ZL *zap.Logger) (*Pool, error) {
	ZL.Info("Creating a new strategy pool.")

	// Make a new pool and set the index and counter.
	sp := Pool{}
	sp.poolIndex = EntryCount - 1
	sp.length = EntryCount

	for i := PoolStart; i < EntryCount; i++ {
		// Calculate the pool range.
		start := PoolStart + i*ReservedPerPool
		end := PoolStart + (i+1)*ReservedPerPool - 1

		// Add a strategy for each entry.
		st, err := NewStrategy(Epoch, start, end, ZL)
		if err != nil {
			ZL.Fatal("Failed to create a strategy pool.")
			return &sp, err
		}
		sp.Pool = append(sp.Pool, st)
	}

	return &sp, nil
}

// Requests the next strategy instance from the pool. These are
// doled out using a round-robin method.
func (sp *Pool) Next() *Strategy {
	sp.mutex.Lock()
	defer sp.mutex.Unlock()

	// Round-robin.
	sp.poolIndex++
	if sp.poolIndex >= sp.length {
		sp.poolIndex = 0
	}

	return sp.Pool[sp.poolIndex]
}

// Create a new instance of a strategy.
// Not thread-safe.
func NewStrategy(Epoch time.Time, PoolMinimum uint32, PoolMaximum uint32, ZL *zap.Logger) (*Strategy, error) {
	entries := PoolMaximum - PoolMinimum + 1

	st := new(Strategy{
		threadID:           0,
		wasExhausted:       false,
		exhaustedTimestamp: 0,
		reserveQueue:       make(reservePoolType, 0, entries),
		exhaustedQueue:     make(reservePoolType, 0, entries),
		exhaustedMax:       0,
		idCount:            0,
		zl:                 ZL})

	// Create a pool of IDS to hold in reserve.
	for i := PoolMinimum; i <= PoolMaximum; i++ {
		st.reserveQueue = append(st.reserveQueue, PoolMinimum+i)
	}

	// Get an initial threadID.
	st.reserveQueue, st.threadID = dequeue(st.reserveQueue, st.zl)

	// Make a new snowflake to use.
	sm, err := snowflake.NewMachine(Epoch, st.threadID)
	st.snowmachine = sm

	return st, err
}

// Use a strategy to get a unique identifier. This function is thread-safe.
func (st *Strategy) NextID() snowflake.Flake {
	// Placing a mutex on this entire function will allow multiple
	// clients to share the same strategy. You can comment this out
	// if you're sure each execution thread / goroutine will have
	// its own strategy instance.
	st.mutex.Lock()
	defer st.mutex.Unlock()

	// Get the next ID available.
	id, isExhausted := st.snowmachine.NextID()
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
		st.reserveQueue, st.threadID = dequeue(st.reserveQueue, st.zl)
		previousID := st.snowmachine.ResetID(st.threadID)
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
