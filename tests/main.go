package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ivanhawkes/snowflake/strategy"
	"go.uber.org/zap"
)

type threadIDMapType map[uint64]uint64

// Maps are not threadsafe, so you need to surround operations on them
// with a mutex to avoid concurrency issues.
type SafeMap struct {
	mutex sync.RWMutex
	m     threadIDMapType
}

func (sm *SafeMap) Exists(key uint64) bool {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	_, exists := sm.m[key]

	return exists
}

func (sm *SafeMap) Set(key uint64) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.m[key] = key
}

var m SafeMap

func burnID(count int, sp *strategy.StrategyPool, wg *sync.WaitGroup, ZL *zap.Logger) {
	defer wg.Done()

	for i := 0; i < count; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		time.Sleep(time.Microsecond * 1)

		// Round robin the strategies.
		st := sp.Next()

		// Get the next snowflake.
		id := st.NextID()

		// Track all the IDs we've generated for test purposes.
		if !m.Exists(id) {
			m.Set(id)
		} else {
			ZL.Fatal("Thread ID already exists.", zap.Uint64("id", id))
		}
	}
}

func main() {
	start := time.Now()

	// Provide a logging solution.
	zl, _ := zap.NewProduction()
	defer zl.Sync()

	var wg sync.WaitGroup

	// By inserting the values into a map we can easily check for duplicates.
	m.m = make(threadIDMapType)

	// Set the epoch to zero, the built-in default value will be used.
	epoch := time.Time{}

	// Create a pool of strategies to work with.
	sp, err := strategy.NewStrategyPool(epoch, 24, 0, 16, zl)
	if err != nil {
		zl.Fatal("Failed to create a snowflake strategy pool.")
	}

	// Let them know which epoch is in use.
	fmt.Printf("Epoch: %v\n\n", epoch)

	// // Fire up a bunch of goroutines to churn out IDs.
	for range 24 {
		wg.Add(1)
		go burnID(8192, sp, &wg, zl)
	}

	// Wait for them to all finish.
	wg.Wait()

	end := time.Now()

	// Print out the statistics for the run.
	var total int = 0
	for _, st := range sp.Pool {
		st.PrintStatistics()
		total += int(st.GetStatistics().IdCount)
	}

	// How many, how fast.
	fmt.Printf("\nTOTAL: %d IDs created in %v\n", len(m.m), end.Sub(start))
}
