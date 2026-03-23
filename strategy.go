package snowflake

// Example strategy for dealing with key exhaustion.
//
// e.g.
//
// SlashAndBurn - just keep getting new ones and roll through the entire
// allocation in order, then repeat.
//
// ReturnToBase - have one main ID, burn some as needed, and return to that ID
// when the 'isExhausted' flag is reset to false. Keep the IDs in order, and
// use a table scan to allocate them. This keeps as many as possible free
// from use in case we blow through the entire range of years running this.
