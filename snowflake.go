package snowflake

import (
	"errors"
	"net"
	"sync"
	"time"
)

// InterfaceAddrs defines the interface used for retrieving network addresses.
type InterfaceAddrs func() ([]net.Addr, error)

// These constants are the bit lengths of Snowflake ID parts.
const (
	BitLenTime      = 39                               // bit length of time
	BitLenSequence  = 10                               // bit length of sequence number
	BitLenMachineID = 63 - BitLenTime - BitLenSequence // bit length of machine id
)

// Settings configures Snowflake:
//
// StartTime is the time since which the Snowflake time is defined as the elapsed time.
// If StartTime is 0, the start time of the Snowflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If StartTime is ahead of the current time, Snowflake is not created.
//
// MachineID returns the unique ID of the Snowflake instance.
// If MachineID returns an error, Snowflake is not created.
// If MachineID is nil, default MachineID is used.
// Default MachineID returns the lower 16 bits of the private IP address.
//
// CheckMachineID validates the uniqueness of the machine ID.
// If CheckMachineID returns false, Snowflake is not created.
// If CheckMachineID is nil, no validation is done.
type Settings struct {
	StartTime      time.Time
	MachineID      func() (uint16, error)
	CheckMachineID func(uint16) bool
}

type Snowflake struct {
	mutex       *sync.Mutex
	startTime   int64
	elapsedTime int64
	sequence    uint16
	machineID   uint16
}

var (
	ErrStartTimeAhead   = errors.New("start time is ahead of now")
	ErrNoPrivateAddress = errors.New("no private ip address")
	ErrOverTimeLimit    = errors.New("over the time limit")
	ErrInvalidMachineID = errors.New("invalid machine id")
	ErrInvalidSequence  = errors.New("invalid sequence number")
)

var defaultInterfaceAddrs = net.InterfaceAddrs

// New returns a new Snowflake configured with the given Settings.
// New returns an error in the following cases:
// - Settings.StartTime is ahead of the current time.
// - Settings.MachineID returns an error.
// - Settings.CheckMachineID returns false.
func New(st Settings) (*Snowflake, error) {
	if st.StartTime.After(time.Now()) {
		return nil, ErrStartTimeAhead
	}

	sf := new(Snowflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<BitLenSequence - 1)

	if st.StartTime.IsZero() {
		sf.startTime = toSnowflakeTime(time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC))
	} else {
		sf.startTime = toSnowflakeTime(st.StartTime)
	}

	var err error
	if st.MachineID == nil {
		sf.machineID, err = lower16BitPrivateIP(defaultInterfaceAddrs)
	} else {
		sf.machineID, err = st.MachineID()
	}
	if err != nil {
		return nil, err
	}

	if st.CheckMachineID != nil && !st.CheckMachineID(sf.machineID) {
		return nil, ErrInvalidMachineID
	}

	return sf, nil
}

// NewSnowflake returns a new Snowflake configured with the given Settings.
// NewSnowflake returns nil in the following cases:
// - Settings.StartTime is ahead of the current time.
// - Settings.MachineID returns an error.
// - Settings.CheckMachineID returns false.
func NewSnowflake(st Settings) *Snowflake {
	sf, _ := New(st)
	return sf
}

// NextID generates a next unique ID.
// After the Snowflake time overflows, NextID returns an error.
func (sf *Snowflake) NextID() (uint64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)

	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	current := currentElapsedTime(sf.startTime)
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

const SnowflakeTimeUnit = 1e7 // nsec, i.e. 10 msec

func toSnowflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / SnowflakeTimeUnit
}

func currentElapsedTime(startTime int64) int64 {
	return toSnowflakeTime(time.Now()) - startTime
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime*SnowflakeTimeUnit) -
		time.Duration(time.Now().UTC().UnixNano()%SnowflakeTimeUnit)
}

func (sf *Snowflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<BitLenTime {
		return 0, ErrOverTimeLimit
	}

	return uint64(sf.elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(sf.sequence)<<BitLenMachineID |
		uint64(sf.machineID), nil
}

func privateIPv4(interfaceAddrs InterfaceAddrs) (net.IP, error) {
	as, err := interfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, ErrNoPrivateAddress
}

func isPrivateIPv4(ip net.IP) bool {
	// Allow private IP addresses (RFC1918) and link-local addresses (RFC3927)
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168 || ip[0] == 169 && ip[1] == 254)
}

func lower16BitPrivateIP(interfaceAddrs InterfaceAddrs) (uint16, error) {
	ip, err := privateIPv4(interfaceAddrs)
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func ElapsedTime(id uint64) time.Duration {
	return time.Duration(elapsedTime(id) * SnowflakeTimeUnit)
}

func elapsedTime(id uint64) uint64 {
	return id >> (BitLenSequence + BitLenMachineID)
}

func SequenceNumber(id uint64) uint64 {
	const maskSequence = uint64((1<<BitLenSequence - 1) << BitLenMachineID)
	return id & maskSequence >> BitLenMachineID
}

func MachineID(id uint64) uint64 {
	const maskMachineID = uint64(1<<BitLenMachineID - 1)
	return id & maskMachineID
}

func Compose(sf *Snowflake, t time.Time, sequence uint16, machineID uint16) (uint64, error) {
	elapsedTime := toSnowflakeTime(t.UTC()) - sf.startTime

	if elapsedTime < 0 {
		return 0, ErrStartTimeAhead
	}

	if elapsedTime >= 1<<BitLenTime {
		return 0, ErrOverTimeLimit
	}

	if sequence >= 1<<BitLenSequence {
		return 0, ErrInvalidSequence
	}

	return uint64(elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(sequence)<<BitLenMachineID |
		uint64(machineID), nil
}

func Decompose(id uint64) map[string]uint64 {
	msb := id >> 63
	time := elapsedTime(id)
	sequence := SequenceNumber(id)
	machineID := MachineID(id)
	return map[string]uint64{
		"id":         id,
		"msb":        msb,
		"time":       time,
		"sequence":   sequence,
		"machine-id": machineID,
	}
}
