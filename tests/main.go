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

	epoch := time.Time{}
	st, err := strategy.NewStrategy(epoch, 0, 63)
	if err != nil {
		fmt.Println("Failed to create a snowflake strategy.")
	}

	fmt.Printf("Epoch: %v\n", epoch)

	for i := 0; i < 8192; i++ {
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
	st.PrintStatistics()
}
