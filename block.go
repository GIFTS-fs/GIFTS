package gifts

// Block is one fixed-size block stored by GIFTS,
// WARN: the performance depends on the slice type,
// it must be not raw data to be copied around
type Block []byte

func NBlocks(fsize int) (n int) {
	n = fsize / GiftsBlockSize
	if fsize%GiftsBlockSize != 0 {
		n++
	}
	return
}
