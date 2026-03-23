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
)

// Settings configures Snowflake:
//
// Epoch is the time since which the Snowflake time is defined as the elapsed time.
// If Epoch is 0, the start time of the Snowflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If Epoch is ahead of the current time, Snowflake is not created.
//
// ThreadID returns the unique ID of the Snowflake instance.
// If ThreadID returns an error, Snowflake is not created.
// If ThreadID is nil, default ThreadID is used.
// Default ThreadID returns the lower 16 bits of the private IP address.
//
// CheckThreadID validates the uniqueness of the machine ID.
// If CheckThreadID returns false, Snowflake is not created.
// If CheckThreadID is nil, no validation is done.
type Settings struct {
	Epoch         time.Time
	ThreadID      func() (uint16, error)
	CheckThreadID func(uint16) bool
}

type Snowflake struct {
	mutex       *sync.Mutex
	epoch       int64
	elapsedTime int64
	sequence    uint16
	threadID    uint16
}

var (
	ErrEpochAhead      = errors.New("Epoch is ahead of now ()")
	ErrOverTimeLimit   = errors.New("Exceeded the time limit")
	ErrInvalidThreadID = errors.New("Invalid thread id")
)

// New returns a new Snowflake configured with the given Settings.
// New returns an error in the following cases:
// - Settings.Epoch is ahead of the current time.
// - Settings.ThreadID returns an error.
// - Settings.CheckThreadID returns false.
func New(st Settings) (*Snowflake, error) {
	if st.Epoch.After(time.Now()) {
		return nil, ErrEpochAhead
	}

	sf := new(Snowflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<sequenceBitLength - 1)

	if st.Epoch.IsZero() {
		sf.epoch = toSnowflakeTime(time.Date(epochYear, epochMonth, epochDay, 0, 0, 0, 0, time.UTC))
	} else {
		sf.epoch = toSnowflakeTime(st.Epoch)
	}

	var err error
	sf.threadID, err = st.ThreadID()
	if err != nil {
		return nil, err
	}

	if st.CheckThreadID != nil && !st.CheckThreadID(sf.threadID) {
		return nil, ErrInvalidThreadID
	}

	return sf, nil
}

// NextID generates a next unique ID.
// After the Snowflake time overflows, NextID returns an error.
func (sf *Snowflake) NextID() (uint64, error) {
	const maskSequence = uint16(1<<sequenceBitLength - 1)

	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	current := currentElapsedTime(sf.epoch)
	if sf.elapsedTime < current {
		sf.elapsedTime = current
		sf.sequence = 0
	} else {
		sf.sequence = (sf.sequence + 1) & maskSequence
		if sf.sequence == 0 {
			sf.elapsedTime++
			overtime := sf.elapsedTime - current
			time.Sleep(sleepTime((overtime)))
		}
	}

	return sf.toID()
}

func toSnowflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / timeUnit
}

func currentElapsedTime(epoch int64) int64 {
	return toSnowflakeTime(time.Now()) - epoch
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime*timeUnit) -
		time.Duration(time.Now().UTC().UnixNano()%timeUnit)
}

func (sf *Snowflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<timestampBitLength {
		return 0, ErrOverTimeLimit
	}

	return uint64(sf.elapsedTime)<<(threadBitLength+sequenceBitLength) |
		uint64(sf.sequence)<<threadBitLength |
		uint64(sf.threadID), nil
}

func ElapsedTime(id uint64) time.Duration {
	return time.Duration(elapsedTime(id) * timeUnit)
}

func elapsedTime(id uint64) uint64 {
	return id >> (threadBitLength + sequenceBitLength)
}

func SequenceNumber(id uint64) uint64 {
	const maskSequence = uint64((1<<sequenceBitLength - 1) << threadBitLength)

	return id & maskSequence >> threadBitLength
}

func ThreadID(id uint64) uint64 {
	const maskThreadID = uint64(1<<threadBitLength - 1)

	return id & maskThreadID
}
