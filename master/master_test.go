package master

import (
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

	rEmpty := structure.FileCreateReq{Fname: "empty", Fsize: 0, Rfactor: 0}

	af(mEmpty.Create(&rEmpty, &a) == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	af(mEmpty.Lookup("empty", &fb) == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{Fname: "f1", Fsize: 1, Rfactor: 1}

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

	r2 := structure.FileCreateReq{Fname: "f2", Fsize: gifts.GiftsBlockSize + 1, Rfactor: 1}

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

	rEmpty := structure.FileCreateReq{Fname: "empty", Fsize: 0, Rfactor: 0}

	af(mOne.Create(&rEmpty, &a) == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	af(mOne.Lookup("empty", &fb) == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{Fname: "f1", Fsize: 1, Rfactor: 1}

	af(mOne.Create(&r1, &a) == nil, "Create 1 block file failed")
	t.Logf("f1 block: %v\n", a)
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

	r2 := structure.FileCreateReq{Fname: "f2", Fsize: gifts.GiftsBlockSize + 1, Rfactor: 1}

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
	ServRPC(mmEmpty, "localhost:3001")
	mEmpty := NewConn("localhost:3001")

	var err error
	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{Fname: "empty", Fsize: 0, Rfactor: 0}

	a, err = mEmpty.Create(rEmpty.Fname, rEmpty.Fsize, rEmpty.Rfactor)
	af(err == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	fb, err = mEmpty.Lookup("empty")
	af(err == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{Fname: "f1", Fsize: 1, Rfactor: 1}

	a, err = mEmpty.Create(r1.Fname, r1.Fsize, r1.Rfactor)
	af(err == nil, "Create 1 block file failed")
	t.Logf("f1 block: %v\n", a)
	af(len(a) == 1, "1 byte should have 1 block")
	af(len(a[0].BlockID) > 0, "bolck ID must be a non-empty string")
	af(len(a[0].Replicas) == 0, "empty master have no replicas to assign")

	fb, err = mEmpty.Lookup("f1")
	af(err == nil, "Lookup f1 failed")
	af(fb.Fsize == 1, "lookup f1 should have 1 byte in size")
	af(len(fb.Assignments) == 1, "lookup f1 should have 1 block assignment")
	af(fb.Assignments[0].BlockID == a[0].BlockID, "lookup f1 should have same blockID")
	af(len(fb.Assignments[0].Replicas) == 0, "empty master have no replicas to lookup")

	r2 := structure.FileCreateReq{Fname: "f2", Fsize: gifts.GiftsBlockSize + 1, Rfactor: 1}

	a, err = mEmpty.Create(r2.Fname, r2.Fsize, r2.Rfactor)
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
	ServRPC(mmOne, "localhost:3001")
	mOne := NewConn("localhost:3001")

	var err error
	var a []structure.BlockAssign
	var fb *structure.FileBlocks

	rEmpty := structure.FileCreateReq{Fname: "empty", Fsize: 0, Rfactor: 0}

	a, err = mOne.Create(rEmpty.Fname, rEmpty.Fsize, rEmpty.Rfactor)
	af(err == nil, "Create empty file failed")
	af(len(a) == 0, "Empty file should have 0 blocks")

	fb, err = mOne.Lookup("empty")
	af(err == nil, "Lookup empty file failed")
	af(fb.Fsize == 0, "Empty file has size 0")
	af(len(fb.Assignments) == 0, "Empty file has no assignments")

	r1 := structure.FileCreateReq{Fname: "f1", Fsize: 1, Rfactor: 1}

	a, err = mOne.Create(r1.Fname, r1.Fsize, r1.Rfactor)
	af(err == nil, "Create 1 block file failed")
	t.Logf("f1 block: %v\n", a)
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

	r2 := structure.FileCreateReq{Fname: "f2", Fsize: gifts.GiftsBlockSize + 1, Rfactor: 1}

	a, err = mOne.Create(r2.Fname, r2.Fsize, r2.Rfactor)
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
