package go_db

func checkBitFromData(bitmap []byte, index int) bool {
	return bool((int(bitmap[index/8]) & (1 << (index % 8))) != 0)
}

func checkBit(db *openDB, bitmap dbPointer, index int) bool {
	if index >= int(bitmap.size)*8 {
		return false // out of range
	}
	// TODO: use offset once it is implemented in readFromDbPointer
	containingByte := uint32(index / 8)
	data := readFromDbPointer(db, bitmap, containingByte+1)
	return checkBitFromData(data, index)
}

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

func writeBitToBitmap(db *openDB, bitmapPointerOffset int64, index uint32, newValue uint8) {
	assert(((newValue == 0) || (newValue == 1)), "Illegal bit value "+string(newValue))
	bitmapPointer := getMutableDbPointer(db, uint32(bitmapPointerOffset))
	if index/8 >= bitmapPointer.pointer.size {
		// just add a byte to the bitmap, we'll override it soon
		appendDataToDataBlock(db, make([]byte, 1), uint32(bitmapPointerOffset))
		bitmapPointer.pointer.size += 1
	}
	// Now update the bitmap's data
	bitmapData := readAllDataFromDbPointer(db, bitmapPointer.pointer)
	changedByte := bitmapData[index/8]
	bitOffset := uint8(index % 8)
	changedByte &= (0xff ^ (1 << bitOffset)) // zero out the changed bit
	changedByte |= (newValue << bitOffset)   // write the new value into the byte
	writeToDataBlock(db, bitmapPointer.pointer, []byte{changedByte}, index/8)
}

func allocateNewDataBlock(db *openDB) dbPointer {
	blockBitmap := readAllDataFromDbPointer(db, db.header.bitmapPointer)
	blockIndex := findFirstAvailableBlock(blockBitmap)
	writeBitToBitmap(db, int64(DATABLOCK_BITMAP_POINTER_OFFSET), blockIndex, 1)
	// Now add the actual data block to the file
	blockOffset := int64(blockIndex) * int64(db.header.dataBlockSize)
	db.f.Seek(blockOffset, 0)
	zeroesBuffer := make([]byte, db.header.dataBlockSize) // TODO: validate these are all zeroes
	n, err := db.f.Write(zeroesBuffer)
	check(err)
	assert(n == int(db.header.dataBlockSize), "Failed to write new data block")
	return dbPointer{offset: uint32(blockOffset), size: 0}
}
