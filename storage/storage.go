package storage

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
)

const (
	// RPCPathStorage the path that Storage listens to
	RPCPathStorage = "/_gifts_storage_"
	// a safe measurement
	maxStatDataSize = 500
)

// Storage is a concurrency-safe key-value store.
type Storage struct {
	Logger     *gifts.Logger // PRODUCTION: banish this
	blocks     sync.Map
	blocksLock sync.RWMutex
	rpc        sync.Map

	// stat
	StatEnabled     bool
	statLastCollect time.Time
	statData        []int
	statCounter     int
	statCounterLock sync.Mutex
	statDone        chan bool
}

// NewStorage creates a new storage node
func NewStorage() *Storage {
	return &Storage{
		Logger: gifts.NewLogger("Storage", "local", false), // PRODUCTION: banish this
	}
}

// ServeRPCBlock makes the raw Storage accessible via RPC at the specified IP
// address and port.  It blocks and does not return.
func ServeRPCBlock(s *Storage, addr string, readyChan chan bool) (err error) {
	s.Logger = gifts.NewLogger("Storage", addr, s.Logger.Enabled) // PRODUCTION: banish this

	defer func() {
		if readyChan != nil {
			select {
			case readyChan <- false:
			default:
			}
		}
	}()

	server := rpc.NewServer()
	err = server.Register(s)
	if err != nil {
		s.Logger.Printf("ServeRPC(%q) => %v", addr, err)
		return
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.Logger.Printf("ServeRPC(%q) => %v", addr, err)
		return
	}

	mux := http.NewServeMux()
	mux.Handle(RPCPathStorage, server)

	if readyChan != nil {
		readyChan <- true
		readyChan = nil
	}

	s.Logger.Printf("ServeRPC(%q) => success", addr)
	return http.Serve(listener, mux)
}

// ServeRPC makes the raw Storage accessible via RPC at the specified IP
// address and port.  It internally starts the server in a go routine and
// returns.
func ServeRPC(s *Storage, addr string) (err error) {
	readyChan := make(chan bool)
	go func() {
		err = ServeRPCBlock(s, addr, readyChan)
	}()
	if !<-readyChan && err == nil {
		err = fmt.Errorf("Storage %v at %q not ready", s, addr)
	}
	return
}

// Set sets the data associated with the block's ID
func (s *Storage) Set(kv *structure.BlockKV, ignore *bool) error {
	s.Logger.Printf("Storage.Set(%q, %d bytes)", kv.ID, len(kv.Data))

	// Store data into block
	s.blocks.Store(kv.ID, kv.Data)

	return nil
}

// Get gets the data associated with the block's ID
func (s *Storage) Get(id string, ret *gifts.Block) error {
	go s.hitStat()

	// Clear the return value
	*ret = make([]byte, 0)

	// Load block
	value, found := s.blocks.Load(id)

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", id)
		s.Logger.Printf("Storage.Get(%q) => %q", id, err)
		return err
	}

	// Copy data
	block := value.(gifts.Block)
	*ret = make([]byte, len(block))
	copy(*ret, block)

	s.Logger.Printf("Storage.Get(%q) => %d bytes", id, len(block))
	return nil
}

// Replicate the specified block to the destination Storage node
func (s *Storage) Replicate(kv *structure.ReplicateKV, ignore *bool) error {
	// Load block
	s.blocksLock.RLock()
	block, found := s.blocks.Load(kv.ID)
	s.blocksLock.RUnlock()

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", kv.ID)
		s.Logger.Printf("Storage.Replicate(%q, %q) => %q", kv.ID, kv.Dest, err)
		return err
	}

	// Start an RPC session with the destination and copy the block
	rs, _ := s.rpc.LoadOrStore(kv.Dest, NewRPCStorage(kv.Dest))
	blockKV := structure.BlockKV{ID: kv.ID, Data: block.(gifts.Block)}
	if err := rs.(*RPCStorage).Set(&blockKV); err != nil {
		s.Logger.Printf("Storage.Replicate(%q, %q) => %v", kv.ID, kv.Dest, err)
		return err
	}

	s.Logger.Printf("Storage.Replicate(%q, %q) => success", kv.ID, kv.Dest)
	return nil
}

// Unset deletes the data associated with the block's ID
func (s *Storage) Unset(id string, ignore *bool) error {
	// Load block
	_, found := s.blocks.Load(id)

	// Check if ID exists
	if !found {
		err := fmt.Errorf("Block with ID %s does not exist", id)
		s.Logger.Printf("Storage.Unset(%q) => %q", id, err)
		return err
	}

	// Delete block
	s.blocks.Delete(id)

	s.Logger.Printf("Storage.Unset(%q) => success", id)
	return nil
}

func (s *Storage) hitStat() {
	if s.StatEnabled {
		s.statCounterLock.Lock()
		defer s.statCounterLock.Unlock()
		s.statCounter++
	}
}

// WARN: the counter are by no means accurate
// due to many a context switch envolved
func (s *Storage) CollectStat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.statDone:
			return
		case <-ticker.C:
			if len(s.statData) >= maxStatDataSize {
				return
			}
			s.statCounterLock.Lock()
			s.statData, s.statCounter = append(s.statData, s.statCounter), 0
			s.statCounterLock.Unlock()
		}
	}
}

func (s *Storage) writeStat(prefix string) {
	file, err := os.Create(fmt.Sprintf("%vstat-%d.csv", prefix, time.Now().UnixNano()))
	if err != nil {
		s.Logger.Printf("Failed to create stat file: %v", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	writer.WriteString("Time,Requests/s\n")

	for i, s := range s.statData {
		writer.WriteString(fmt.Sprintf("%v,%v\n", i, s))
	}

}

// TrapSignal traps the system signal and send true to done
func (s *Storage) TrapSignal(prefix string, sigsChan chan os.Signal, done chan bool) {
	<-sigsChan

	// non-blocking send
	select {
	case s.statDone <- true:
	default:
	}

	s.writeStat(prefix)

	done <- true
}
