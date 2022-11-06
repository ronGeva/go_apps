package go_db

import (
	"encoding/binary"
	"os"
)

type IOType int8

const (
	LocalFile IOType = iota
	Network          // not implemented
	InMemory         // not implemented
)

type databaseUniqueID struct {
	ioType            IOType
	identifyingString string
}

// LOCAL DB
/*
Structure of a local DB:
<MAGIC> - 4 bytes
<size of data block> - 4 bytes
<pointer to free blocks bitmap>
<pointer to tables array>

Structure of db pointer:
<offset in the file of data block> - 4 bytes
<total size of data (in all stringed data blocks combined)> - 4 bytes
A null pointer (pointer whose offset equal 0) contains the data itself in the size field,
which allows us to avoid seeking the data in the file in case it is of small, fixed size (for example - int).

Structure of free blocks bitmap:
<bits> - <size of bitmap>
Each bit in the free blocks bitmap represents whether the i-th data block in the db file is free
to use.

Structure of tables array:
<size of tables array> - 4 bytes
<tables array> - <db pointer size> * <size>, each entry is a db pointer

Structure of table:
<MAGIC> - 4 bytes
<size of unique ID> - 4 bytes
<unique id> - variable size
<scheme> - variable size
<records array> - variable size

Structure of table scheme:
<amount of column headers> - 4 bytes
<columns headers> - variable length

Structure of column header:
<column type> - 1 byte
<size of column name> - 4 bytes
<column name> - variable length

Structure of records array:
<records array size> - 4 bytes
<records array> - <db pointer size> * <records table size>
Each cell in the records array contains the offset in the DB file in which the record lies.

Data blocks structure:
A data block is a sequence of <data block size> - 4 bytes of data, whose structure is defined
by the element whose data this block contains.
Following the data is a 4 byte value representing the offset in the file of the next data block,
which allows us to hold an arbitrary long sequence of bytes for every db structure.
*/

const LOCAL_DB_CONST uint32 = 0x1414ffbc
const DB_POINTER_SIZE uint32 = 8
const DATA_BLOCK_SIZE_SIZE uint32 = 4
const LOCAL_DB_CONST_SIZE = 4
const BITMAP_POINTER_OFFSET = LOCAL_DB_CONST_SIZE + DATA_BLOCK_SIZE_SIZE

// According to the description of the DB header
const DB_HEADER_SIZE = LOCAL_DB_CONST_SIZE + DATA_BLOCK_SIZE_SIZE + 2*DB_POINTER_SIZE

type fileDB struct {
	path string
}

type dbPointer struct {
	offset uint32
	size   uint32
}

type dbHeader struct {
	magic         uint32
	dataBlockSize uint32
	bitmapPointer dbPointer
	tablesPointer dbPointer
}

type openDB struct {
	f      *os.File
	header dbHeader
}

func readDataBlock(db *openDB, offset uint32) ([]byte, uint32) {
	dataBlockSize := db.header.dataBlockSize
	if offset%dataBlockSize != 0 {
		panic(BadReadRequestError{})
	}
	db.f.Seek(int64(offset), 0)
	data := make([]byte, dataBlockSize)
	readSize, err := db.f.Read(data)
	check(err)
	if readSize != int(dataBlockSize) {
		panic(ReadError{})
	}
	nextOffset := binary.LittleEndian.Uint32(data[len(data)-4:])
	data = data[:len(data)-4]
	return data, nextOffset
}

func readFromDbPointer(db *openDB, pointer dbPointer) []byte {
	totalSize := pointer.size
	data := make([]byte, totalSize)
	dataBlockSize := db.header.dataBlockSize
	currentOffset := pointer.offset
	for i := uint32(0); i < totalSize/dataBlockSize; i++ {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, nextOffset := readDataBlock(db, currentOffset)
		currentOffset = nextOffset
		copy(data[i*dataBlockSize:], newData)
	}

	if totalSize%dataBlockSize != 0 {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, _ := readDataBlock(db, currentOffset)
		copy(data[totalSize-(totalSize%dataBlockSize):], newData)
	}
	return data
}

func serializeDbPointer(pointer dbPointer) []byte {
	res := make([]byte, DB_POINTER_SIZE)
	binary.LittleEndian.PutUint32(res, pointer.offset)
	binary.LittleEndian.PutUint32(res, pointer.size)
	return res
}

func deserializeDbPointer(data []byte) dbPointer {
	if len(data) < int(DB_POINTER_SIZE) {
		panic(DeserializationError{})
	}

	offset := binary.LittleEndian.Uint32(data[:4])
	size := binary.LittleEndian.Uint32(data[4:8])
	return dbPointer{offset: offset, size: size}
}

func deserializeDbHeader(data []byte) dbHeader {
	if len(data) < int(DB_HEADER_SIZE) {
		panic(DeserializationError{})
	}

	magic := binary.LittleEndian.Uint32(data[:LOCAL_DB_CONST_SIZE])
	if magic != LOCAL_DB_CONST {
		panic(MalformedDBError{})
	}
	dataBlockSize := binary.LittleEndian.Uint32(data[LOCAL_DB_CONST_SIZE : LOCAL_DB_CONST_SIZE+DATA_BLOCK_SIZE_SIZE])
	bitmapPointer := deserializeDbPointer(data[LOCAL_DB_CONST_SIZE+DATA_BLOCK_SIZE_SIZE:])
	tablesPointer := deserializeDbPointer(data[LOCAL_DB_CONST_SIZE+DATA_BLOCK_SIZE_SIZE+DB_POINTER_SIZE:])
	return dbHeader{magic: magic, dataBlockSize: dataBlockSize,
		bitmapPointer: bitmapPointer, tablesPointer: tablesPointer}
}

func findFirstAvailableBlock(bitmap []byte) uint32 {
	res := uint32(0)
	for _, b := range bitmap {
		for b > 0 {
			if (b ^ 1) == 1 {
				return res
			}
			b >>= 1
			res += 1
		}
		res += 8
	}
	return res
}

func appendDataToDataBlock(db *openDB, newData []byte) {
	// TODO: implement
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func writeToDataBlock(db *openDB, pointer dbPointer, data []byte, offset uint32) {
	assert(offset+uint32(len(data)) <= pointer.size, "Cannot write passed end of data block")
	currentBlockOffset := pointer.offset
	for offset > db.header.dataBlockSize {
		_, currentBlockOffset = readDataBlock(db, currentBlockOffset)
		offset -= db.header.dataBlockSize
	}
	offsetInWriteBuffer := 0
	for offsetInWriteBuffer < len(data) {
		db.f.Seek(int64(currentBlockOffset)+int64(offset), 0)
		sizeToWrite := min(int(db.header.dataBlockSize-offset), len(data)-offsetInWriteBuffer)
		offset = 0 // from now on we will always write to the start of the blocks

		n, err := db.f.Write(data[offsetInWriteBuffer : offsetInWriteBuffer+int(sizeToWrite)])
		check(err)
		assert(n == int(sizeToWrite), "")
		offsetInWriteBuffer += int(sizeToWrite)
		_, currentBlockOffset = readDataBlock(db, currentBlockOffset)
	}
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
		appendDataToDataBlock(db, make([]byte, 1))
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

func writeNewTableLocalFile(scheme tableScheme, db database) {
	dbPath := db.id.identifyingString
	// TODO: change this to allow multiple read-writes at the same time
	f, err := os.OpenFile(dbPath, os.O_RDWR, os.ModeExclusive)
	defer f.Close()
	check(err)

	headerData := make([]byte, DB_HEADER_SIZE)
	n, err := f.Read(headerData)
	check(err)
	if n < int(DB_HEADER_SIZE) {
		panic(ReadError{})
	}
	header := deserializeDbHeader(headerData)
	openDatabase := openDB{f: f, header: header}
	newDatabasePointer := allocateNewDataBlock(&openDatabase)
	// TODO: write table into newly allocated data block, update pointer and add it into
	// the tables array
}

func writeNewTable(scheme tableScheme, db database) {
	if db.id.ioType != LocalFile {
		panic(UnsupportedError{})
	}
	writeNewTableLocalFile(scheme, db)
}
