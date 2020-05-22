package master

import "github.com/GIFTS-fs/GIFTS/structure"

// Create a file: assign replicas for the clients to write
func (m *Master) Create(req *structure.FileCreateReq, assignments *[]structure.BlockAssign) error {
	// structure.FileCreateReq{Fname: fname, Fsize: fsize, Rfactor: rfactor},

	return nil
}
