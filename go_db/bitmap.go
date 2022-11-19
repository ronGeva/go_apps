package go_db

func findFirstAvailableBlock(bitmap []byte) uint32 {
	res := uint32(0)
	for _, b := range bitmap {
		if b == 0xff {
			res += 8
			continue
		}

		for i := 0; i < 8; i++ {
			if (b & 1) == 0 {
				return res
			}
			b >>= 1
			res += 1
		}
	}
	return res
}

func writeBitToBitmap(db *openDB, index uint32, newValue uint8) {
	assert(((newValue == 0) || (newValue == 1)), "Illegal bit value "+string(newValue))
	if index/8 >= db.header.bitmapPointer.size {
		// We increase the size of the bitmap, and update it on disk
		db.header.bitmapPointer.size += 1
		db.f.Seek(int64(BITMAP_POINTER_OFFSET), 0)
		bitmapPointerData := serializeDbPointer(db.header.bitmapPointer)
		n, err := db.f.Write(bitmapPointerData)
		check(err)
		if n < int(BITMAP_POINTER_OFFSET) {
			panic(InsufficientWriteError{DB_POINTER_SIZE, n})
		}
		// just add a byte to the bitmap, we'll override it soon
		appendDataToDataBlock(db, make([]byte, 1), BITMAP_POINTER_OFFSET)
	}
	// Now update the bitmap's data
	bitmapData := readFromDbPointer(db, db.header.bitmapPointer)
	changedByte := bitmapData[index/8]
	bitOffset := uint8(index % 8)
	changedByte &= (0xff ^ (1 << bitOffset)) // zero out the changed bit
	changedByte |= (newValue << bitOffset)   // write the new value into the byte
	writeToDataBlock(db, db.header.bitmapPointer, []byte{changedByte}, index/8)
}

func allocateNewDataBlock(db *openDB) dbPointer {
	blockBitmap := readFromDbPointer(db, db.header.bitmapPointer)
	blockIndex := findFirstAvailableBlock(blockBitmap)
	writeBitToBitmap(db, blockIndex, 1)
	// Now add the actual data block to the file
	blockOffset := int64(blockIndex) * int64(db.header.dataBlockSize)
	db.f.Seek(blockOffset, 0)
	zeroesBuffer := make([]byte, db.header.dataBlockSize) // TODO: validate these are all zeroes
	n, err := db.f.Write(zeroesBuffer)
	check(err)
	assert(n == int(db.header.dataBlockSize), "Failed to write new data block")
	return dbPointer{offset: uint32(blockOffset), size: 0}
}
