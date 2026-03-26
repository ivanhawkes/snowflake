package main

import (
	"fmt"
	"log"
	"time"

	"ivanhawkes.dev/snowflake/strategy"
)

type threadIDMapType map[uint64]uint64

func main() {
	// By inserting the values into a map we can easily check for duplicates.
	m := make(threadIDMapType)

	// Set the epoch to zero, the built-in default value will be used.
	epoch := time.Time{}

	// Create a pool of strategies to work with.
	sp, err := strategy.NewStrategyPool(epoch, 4, 0, 16)
	if err != nil {
		log.Fatal("Failed to create a snowflake strategy.")
	}

	// st, err := strategy.NewStrategy(epoch, 0, 63)
	// if err != nil {
	// 	fmt.Println("Failed to create a snowflake strategy.")
	// }

	fmt.Printf("Epoch: %v\n\n", epoch)

	for i := 0; i < 8192; i++ {
		// DEBUG: waste some time so we can test the timestamp bucketing.
		time.Sleep(time.Microsecond)

		// Round robin the strategies.
		st := sp.Next()

		// Get the next snowflake.
		id := st.NextID()

		// Store it in a map for debugging purposes.
		_, exists := m[id]
		if !exists {
			m[id] = id
		} else {
			log.Fatal("--- THREAD ID CONFLICT ---")
		}
	}

	// Print out the statistics for the run.
	for _, st := range sp.Pool {
		st.PrintStatistics()
	}
}
