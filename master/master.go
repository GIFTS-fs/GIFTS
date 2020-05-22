package master

import (
	"sync"

	gifts "github.com/GIFTS-fs/GIFTS"
)

// Master is the master of GIFTS
type Master struct {
	fMap   sync.Map
	logger *gifts.Logger
}

// NewMaster is the constructor for master
func NewMaster() *Master {
	m := Master{}
	m.logger = gifts.NewLogger("Master", "master", true) // PRODUCTION: banish this
	return &m
}

func ServRPC(addr string) {

}
