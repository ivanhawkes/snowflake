package snowflake

import (
	"errors"
	"sync"
	"time"
)

// +-+-------------------------------------+-------------------+-------+
// |x|Time units since epoch               | Thread ID         | Seq.  |
// +-+-------------------------------------+-------------------+-------+

// Bit map organisation:
//
// The first bit is the sign bit. SQL only supports int, not uint, so if you
// want to use this bit you need to accept negative epoch offsets.
//
// The timestamp which is an offset from the epoch takes up the next most
// significant bits, ensuring a generally good sort order.
//
// Next is the sequence ID and thread ID. Twitter used machine ID and datacentre
// ID and made those more significant than the sequence ID. I'm going to combine
// machine ID and datacentre into a single value to potentially reduce the amount
// of wasted bits. Further, it will be a thread ID and each machine will be
// expected to allocate a pool of them to it's running threads.
//
// Pooling is up to the calling code to implement. It's not within the domain
// of this code.
//
// Additionally, thread pools can be used to sidestep a key issue when
// performing a bulk insert on a database. These are typically single
// threaded operations and would soon be forced to throttle because of
// sequence ID exhaustion on the generator for that thread.
//
// By moving this responsibility up the chain the client is free to implement
// whichever strategy makes sense for their organisation.
//
// e.g. keep a reserve of thread IDs which can be chain used by an import
// process to rapidly perform the bulk inserts. Alternatively, since this is
// often done as an offline process, they can use the entire pool of IDs and
// avoid locking altogether.
//
// I have kept thread locks on the increment function because it's quite possible
// some people will want to share the ID generator between multiple threads.

const (
	// The year of the epoch.
	epochYear = 2026

	// The month of the epoch.
	epochMonth = 3

	// The day of the epoch.
	epochDay = 20

	// Time units are buckets into which sequence ID will be place. They are measured
	// in nanoseconds. In this case the units are 10 milliseconds.
	timeUnit = 1e7

	// The number of bits allocated to units of time.
	timestampBitLength = 37

	// The remaining bits make up the thread ID.
	threadBitLength = 19

	// The number of bits allocated to the sequence number.
	sequenceBitLength = 63 - timestampBitLength - threadBitLength

	// A bitmask for the sequence bits. This is also the largest unsigned
	// integer the sequence can represent.
	sequenceMask = 1<<sequenceBitLength - 1
)

type Snowflake struct {
	// The date from which all timestamp offsets are measured.
	epoch int64

	// The time that has passed since the epoch.
	lastTimestamp int64

	// An incrementing counter that places multiple ID requests made in the
	// same timestamp period into the same bucket without duplications.
	// This gets reset to zero when the timestamp changes.
	sequence uint32

	// An ID that is unique for every thread on every machine in the cluster.
	// While it's possible to share a snowflake between threads, best practice
	// is for each thread / coroutine to have it's own ThreadID.
	threadID uint32

	// True when the sequence space for the timestamp period is
	// completely exhauted. The calling program will need to take
	// mitigation steps.
	isExhausted bool

	// A mutex will let us lock the instance of snowflake while goroutines
	// execute code that is not thread safe.
	mutex sync.Mutex
}

var (
	ErrEpochAhead = errors.New("Epoch is ahead of now ()")
)

// Returns a new Snowflake with values computed from the arguments
// and it's constant constraints.
// Errors: Epoch is ahead of the current time.
func New(Epoch time.Time, ThreadID uint32) (*Snowflake, error) {
	if Epoch.After(time.Now()) {
		return nil, ErrEpochAhead
	}

	sf := new(Snowflake)
	sf.sequence = sequenceMask
	sf.isExhausted = false

	if Epoch.IsZero() {
		sf.epoch = toSnowflakeTime(time.Date(epochYear, epochMonth, epochDay, 0, 0, 0, 0, time.UTC))
	} else {
		sf.epoch = toSnowflakeTime(Epoch)
	}

	sf.threadID = ThreadID

	return sf, nil
}

// This snowflake is being assigned a new ThreadID, possibly due to
// exhaustion of the sequence.
func (sf *Snowflake) ResetID(ThreadID uint32) uint32 {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	originalID := sf.threadID

	sf.threadID = ThreadID
	sf.sequence = sequenceMask
	sf.isExhausted = false

	return originalID
}

// NextID generates the next unique ID.
func (sf *Snowflake) NextID() (uint64, bool) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	currentTimestamp := currentElapsedTime(sf.epoch)

	// Has the timestamp changed.
	if currentTimestamp > sf.lastTimestamp {
		// Yes. Roll over to the new timestamp and reset the sequence.
		sf.lastTimestamp = currentTimestamp
		sf.sequence = 0
		sf.isExhausted = false
	} else {
		// No. Increment the sequence counter.
		sf.sequence = (sf.sequence + 1) & sequenceMask

		// Check if the counter is now exhausted.
		if sf.sequence == sequenceMask {
			sf.isExhausted = true
		}
	}

	id := sf.toID()

	return id, sf.isExhausted
}

func toSnowflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / timeUnit
}

func currentElapsedTime(epoch int64) int64 {
	return toSnowflakeTime(time.Now()) - epoch
}

func (sf *Snowflake) toID() uint64 {
	return uint64(sf.lastTimestamp)<<(threadBitLength+sequenceBitLength) |
		uint64(sf.sequence)<<threadBitLength |
		uint64(sf.threadID)
}

func ElapsedTime(id uint64) time.Duration {
	return time.Duration(TimeStamp(id) * timeUnit)
}

func SequenceNumber(id uint64) uint64 {
	const mask = uint64(sequenceMask << threadBitLength)

	return id & mask >> threadBitLength
}

func ThreadID(id uint64) uint64 {
	const maskThreadID = uint64(1<<threadBitLength - 1)

	return id & maskThreadID
}

func TimeStamp(id uint64) uint64 {
	return id >> (threadBitLength + sequenceBitLength)
}
