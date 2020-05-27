package master

import (
	"fmt"
	"testing"

	gifts "github.com/GIFTS-fs/GIFTS"
	"github.com/GIFTS-fs/GIFTS/structure"
	"github.com/GIFTS-fs/GIFTS/test"
)

func TestMasterLocalEmpty(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	mEmpty := NewMaster([]string{})

	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{FName: "empty", FSize: 0, RFactor: 0}

	af(mEmpty.Create(&rEmpty, &a) == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	af(mEmpty.Lookup("empty", &fb) == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{FName: "f1", FSize: 1, RFactor: 1}

	af(mEmpty.Create(&r1, &a) == nil, "Create 1 block file failed")
	t.Logf("f1 block: %v\n", a)
	af(len(a) == 1, "1 byte should have 1 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 0, "empty master have no replicas to assign")

	af(mEmpty.Lookup("f1", &fb) == nil, "Lookup f1 failed")
	af(fb.Fsize == 1, "lookup f1 should have 1 byte in size")
	af(len(fb.Assignments) == 1, "lookup f1 should have 1 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f1 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 0, "empty master have no replicas to lookup")

	r2 := structure.FileCreateReq{FName: "f2", FSize: gifts.GiftsBlockSize + 1, RFactor: 1}

	af(mEmpty.Create(&r2, &a) == nil, "Create 2 block file failed")
	af(len(a) == 2, "blocksize+1 byte should have 2 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 0, "empty master have no replicas to assign")
	af(len(a[1].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[1].Replicas) == 0, "empty master have no replicas to assign")

	af(mEmpty.Lookup("f2", &fb) == nil, "Lookup f2 failed")
	af(fb.Fsize == gifts.GiftsBlockSize+1, "lookup f2 should have blocksize+1 byte in size")
	af(len(fb.Assignments) == 2, "lookup f2 should have 2 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f2 should have same blockID")
	af(fb.Assignments[1].BlockID == a[1].BlockID, "lookup f2 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 0, "empty master have no replicas to lookup")
	af(len(fb.Assignments[1].Replicas) == 0, "empty master have no replicas to lookup")
}

func TestMasterLocalOne(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	mOne := NewMaster([]string{"s1"})

	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{FName: "empty", FSize: 0, RFactor: 0}

	af(mOne.Create(&rEmpty, &a) == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	af(mOne.Lookup("empty", &fb) == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{FName: "f1", FSize: 1, RFactor: 1}

	af(mOne.Create(&r1, &a) == nil, "Create 1 block file failed")
	af(len(a) == 1, "1 byte should have 1 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 1, "one master have one replica to assign")
	af(a[0].Replicas[0] == "s1", "one master have only one replica to assign")

	af(mOne.Lookup("f1", &fb) == nil, "Lookup f1 failed")
	af(fb.Fsize == 1, "lookup f1 should have 1 byte in size")
	af(len(fb.Assignments) == 1, "lookup f1 should have 1 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f1 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 1, "one master have one replica to lookup")
	af(fb.Assignments[0].Replicas[0] == "s1", "one master have only one replica to lookup")

	r2 := structure.FileCreateReq{FName: "f2", FSize: gifts.GiftsBlockSize + 1, RFactor: 1}

	af(mOne.Create(&r2, &a) == nil, "Create 2 block file failed")
	af(len(a) == 2, "blocksize+1 byte should have 2 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 1, "one master have one replicas to assign")
	af(a[0].Replicas[0] == "s1", "one master have only one replica to assign")
	af(len(a[1].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[1].Replicas) == 1, "one master have one replicas to assign")
	af(a[1].Replicas[0] == "s1", "one master have only one replica to assign")

	af(mOne.Lookup("f2", &fb) == nil, "Lookup f2 failed")
	af(fb.Fsize == gifts.GiftsBlockSize+1, "lookup f2 should have blocksize+1 byte in size")
	af(len(fb.Assignments) == 2, "lookup f2 should have 2 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f2 should have same blockID")
	af(fb.Assignments[1].BlockID == a[1].BlockID, "lookup f2 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 1, "one master have one replicas to lookup")
	af(fb.Assignments[0].Replicas[0] == "s1", "one master have only one replicas to lookup")
	af(len(fb.Assignments[1].Replicas) == 1, "one master have one replicas to lookup")
	af(fb.Assignments[1].Replicas[0] == "s1", "one master have only one replicas to lookup")
}

func TestMasterRPCEmpty(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	mmEmpty := NewMaster([]string{})
	ServeRPC(mmEmpty, "localhost:4001")
	mEmpty := NewConn("localhost:4001")

	var err error
	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{FName: "empty", FSize: 0, RFactor: 0}

	a, err = mEmpty.Create(rEmpty.FName, rEmpty.FSize, rEmpty.RFactor)
	af(err == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	fb, err = mEmpty.Lookup("empty")
	af(err == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{FName: "f1", FSize: 1, RFactor: 1}

	a, err = mEmpty.Create(r1.FName, r1.FSize, r1.RFactor)
	af(err == nil, "Create 1 block file failed")
	af(len(a) == 1, "1 byte should have 1 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 0, "empty master have no replicas to assign")

	fb, err = mEmpty.Lookup("f1")
	af(err == nil, "Lookup f1 failed")
	af(fb.Fsize == 1, "lookup f1 should have 1 byte in size")
	af(len(fb.Assignments) == 1, "lookup f1 should have 1 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f1 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 0, "empty master have no replicas to lookup")

	r2 := structure.FileCreateReq{FName: "f2", FSize: gifts.GiftsBlockSize + 1, RFactor: 1}

	a, err = mEmpty.Create(r2.FName, r2.FSize, r2.RFactor)
	af(err == nil, "Create 2 block file failed")
	af(len(a) == 2, "blocksize+1 byte should have 2 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 0, "empty master have no replicas to assign")
	af(len(a[1].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[1].Replicas) == 0, "empty master have no replicas to assign")

	fb, err = mEmpty.Lookup("f2")
	af(err == nil, "Lookup f2 failed")
	af(fb.Fsize == gifts.GiftsBlockSize+1, "lookup f2 should have blocksize+1 byte in size")
	af(len(fb.Assignments) == 2, "lookup f2 should have 2 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f2 should have same blockID")
	af(fb.Assignments[1].BlockID == a[1].BlockID, "lookup f2 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 0, "empty master have no replicas to lookup")
	af(len(fb.Assignments[1].Replicas) == 0, "empty master have no replicas to lookup")
}

func TestMasterRPCOne(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	mmOne := NewMaster([]string{"s1"})
	ServeRPC(mmOne, "localhost:4002")
	mOne := NewConn("localhost:4002")

	var err error
	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{FName: "empty", FSize: 0, RFactor: 0}

	a, err = mOne.Create(rEmpty.FName, rEmpty.FSize, rEmpty.RFactor)
	af(err == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	fb, err = mOne.Lookup("empty")
	af(err == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{FName: "f1", FSize: 1, RFactor: 1}

	a, err = mOne.Create(r1.FName, r1.FSize, r1.RFactor)
	af(err == nil, "Create 1 block file failed")
	af(len(a) == 1, "1 byte should have 1 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 1, "one master have one replica to assign")
	af(a[0].Replicas[0] == "s1", "one master have only one replica to assign")

	fb, err = mOne.Lookup("f1")
	af(err == nil, "Lookup f1 failed")
	af(fb.Fsize == 1, "lookup f1 should have 1 byte in size")
	af(len(fb.Assignments) == 1, "lookup f1 should have 1 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f1 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 1, "one master have one replica to lookup")
	af(fb.Assignments[0].Replicas[0] == "s1", "one master have only one replica to lookup")

	r2 := structure.FileCreateReq{FName: "f2", FSize: gifts.GiftsBlockSize + 1, RFactor: 1}

	a, err = mOne.Create(r2.FName, r2.FSize, r2.RFactor)
	af(err == nil, "Create 2 block file failed")
	af(len(a) == 2, "blocksize+1 byte should have 2 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 1, "one master have one replicas to assign")
	af(a[0].Replicas[0] == "s1", "one master have only one replica to assign")
	af(len(a[1].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[1].Replicas) == 1, "one master have one replicas to assign")
	af(a[1].Replicas[0] == "s1", "one master have only one replica to assign")

	fb, err = mOne.Lookup("f2")
	af(err == nil, "Lookup f2 failed")
	af(fb.Fsize == gifts.GiftsBlockSize+1, "lookup f2 should have blocksize+1 byte in size")
	af(len(fb.Assignments) == 2, "lookup f2 should have 2 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f2 should have same blockID")
	af(fb.Assignments[1].BlockID == a[1].BlockID, "lookup f2 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 1, "one master have one replicas to lookup")
	af(fb.Assignments[0].Replicas[0] == "s1", "one master have only one replicas to lookup")
	af(len(fb.Assignments[1].Replicas) == 1, "one master have one replicas to lookup")
	af(fb.Assignments[1].Replicas[0] == "s1", "one master have only one replicas to lookup")
}

func TestMaster_Create(t *testing.T) {
	af := func(cond bool, msg string) {
		test.AF(t, cond, msg)
	}

	verifyAssignments := func(m *Master, request structure.FileCreateReq, clock int, assignments []structure.BlockAssign) {
		for i := range assignments {
			blockID := fmt.Sprintf("%s%d", request.FName, i)
			af(blockID == assignments[i].BlockID, fmt.Sprintf("Expected block name %q, found %q", blockID, assignments[0].BlockID))

			for _, replica := range assignments[i].Replicas {
				expectedReplica := m.storages[clock].Addr
				af(expectedReplica == replica, fmt.Sprintf("Expected replica %q, found %q", expectedReplica, replica))
				clock = (clock + 1) % len(m.storages)
			}
		}
	}

	var assignments []structure.BlockAssign
	var request structure.FileCreateReq
	var err error
	var fName string
	var clock int
	m := NewMaster([]string{"s1", "s2", "s3", "s4", "s5", "s6"})

	// Requested RFactor is 0
	fName = "large-RFactor"
	request = structure.FileCreateReq{FName: fName, FSize: 10, RFactor: 0}
	err = m.Create(&request, &assignments)
	af(err != nil, "Master should not accept RFactor=0")

	// Requested RFactor is too large
	// DLAD: Why is MaxRFactor a constant?  Shouldn't it be based on the number of storage nodes?
	if false {
		fName = "large-RFactor"
		request = structure.FileCreateReq{FName: fName, FSize: 10, RFactor: uint(len(m.storages) + 1)}
		err = m.Create(&request, &assignments)
		af(err != nil, "Master should not accept RFactor sizes that are larger than the number of storage nodes")
	}

	// File with same name already exists
	fName = "duplicate"
	request = structure.FileCreateReq{FName: fName, FSize: 10, RFactor: 1}
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))

	err = m.Create(&request, &assignments)
	af(err != nil, "Master should not create duplicate file names")

	// Create empty file with 1 replica
	fName = "empty"
	request = structure.FileCreateReq{FName: fName, FSize: 0, RFactor: 1}
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 0, "Empty file should have 0 blocks")

	// Create file with less than one block of data with 1 replica
	fName = "less-than-one-block"
	request = structure.FileCreateReq{FName: fName, FSize: gifts.GiftsBlockSize - 1, RFactor: 1}
	clock = m.createClockHand
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 1, fmt.Sprintf("Expected 1 blocks, found %d", len(assignments)))
	verifyAssignments(m, request, clock, assignments)

	// Create file with exactly one block of data with 1 replica
	fName = "one-block"
	request = structure.FileCreateReq{FName: fName, FSize: gifts.GiftsBlockSize, RFactor: 1}
	clock = m.createClockHand
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 1, fmt.Sprintf("Expected 1 blocks, found %d", len(assignments)))
	verifyAssignments(m, request, clock, assignments)

	// Create file with more than one block of data with 1 replica
	fName = "more-than-one-block"
	request = structure.FileCreateReq{FName: fName, FSize: 3*gifts.GiftsBlockSize + 1, RFactor: 1}
	clock = m.createClockHand
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 4, fmt.Sprintf("Expected 4 blocks, found %d", len(assignments)))
	verifyAssignments(m, request, clock, assignments)

	// Create empty file with multiple replicas
	fName = "empty-replicate"
	request = structure.FileCreateReq{FName: fName, FSize: 0, RFactor: 3}
	clock = m.createClockHand
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 0, "Empty file should have 0 blocks")
	verifyAssignments(m, request, clock, assignments)

	// Create file with more than one block of data with multiple replicas
	fName = "more-than-one-block-replicate"
	request = structure.FileCreateReq{FName: fName, FSize: 3*gifts.GiftsBlockSize + 1, RFactor: 2}
	clock = m.createClockHand
	err = m.Create(&request, &assignments)
	af(err == nil, fmt.Sprintf("Master.Create failed: %v", err))
	af(len(assignments) == 4, fmt.Sprintf("Expected 4 blocks, found %d", len(assignments)))
	verifyAssignments(m, request, clock, assignments)
}
