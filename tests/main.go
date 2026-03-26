package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"ivanhawkes.dev/snowflake/strategy"
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

func burnID(count int, sp *strategy.StrategyPool, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < count; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		time.Sleep(time.Microsecond * 10)

		// Round robin the strategies.
		st := sp.Next()

		// Get the next snowflake.
		id := st.NextID()

		// Track all the IDs we've generated for test purposes.
		if !m.Exists(id) {
			m.Set(id)
		} else {
			log.Fatal("--- THREAD ID CONFLICT ---")
		}
	}
}

func main() {
	start := time.Now()

	var wg sync.WaitGroup

	// By inserting the values into a map we can easily check for duplicates.
	m.m = make(threadIDMapType)

	// Set the epoch to zero, the built-in default value will be used.
	epoch := time.Time{}

	// Create a pool of strategies to work with.
	sp, err := strategy.NewStrategyPool(epoch, 8, 0, 16)
	if err != nil {
		log.Fatal("Failed to create a snowflake strategy.")
	}

	// Let them know which epoch is in use.
	fmt.Printf("Epoch: %v\n\n", epoch)

	// // Fire up a bunch of goroutines to churn out IDs.
	for range 24 {
		wg.Add(1)
		go burnID(8192, sp, &wg)
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
	fmt.Printf("\n\nTOTAL: %d IDs created in %v\n", len(m.m), end.Sub(start))
}
