package go_db

import (
	"encoding/binary"
	"io"
	"os"
)

type IOType int8

const (
	LocalFile IOType = iota
	Network          // not implemented
	InMemory         // not implemented
)

type fileDB struct {
	path string
}

type dbPointer struct {
	offset uint32
	size   uint32
}

type mutableDbPointer struct {
	pointer  dbPointer
	location int64
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

// DB IO utils

func uint32ToBytes(num uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, num)
	return b
}

func readFromFile(f *os.File, size uint32, offset uint32) []byte {
	f.Seek(int64(offset), 0)

	pointerData := make([]byte, size)
	sizeRead, err := f.Read(pointerData)
	check(err)
	assert(sizeRead == int(size), "Failed to read the size requested from db file")
	return pointerData
}

func readFromDB(db *openDB, size uint32, offset uint32) []byte {
	return readFromFile(db.f, size, offset)
}

func serializeVariableSizeData(data []byte) []byte {
	res := make([]byte, len(data)+4)
	binary.LittleEndian.PutUint32(res[:4], uint32(len(data)))
	copy(res[4:], data)
	return res
}

func writeVariableSizeDataToDB(db *openDB, data []byte, offset uint32) {
	dataWithSizePrefix := serializeVariableSizeData(data)
	db.f.Seek(int64(offset), 0)
	db.f.Write(dataWithSizePrefix)
}

func readVariableSizeDataFromDB(db *openDB, offset uint32) []byte {
	sizeBytes := readFromDB(db, 4, offset)
	size := binary.LittleEndian.Uint32(sizeBytes)
	return readFromDB(db, size, offset+4)
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

func readFromDbPointer(db *openDB, pointer dbPointer, size uint32) []byte {
	if pointer.offset == 0 {
		res := make([]byte, 4)
		binary.LittleEndian.PutUint32(res, pointer.size)
		return res // the size contains the actual value
	}

	data := make([]byte, size)
	dataBlockSize := db.header.dataBlockSize
	currentOffset := pointer.offset
	// Read all but the last data block (if it is partial)
	for i := uint32(0); i < size/dataBlockSize; i++ {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, nextOffset := readDataBlock(db, currentOffset)
		currentOffset = nextOffset
		copy(data[i*dataBlockSize:], newData)
	}

	// Read the last partial data block (if there is one)
	if size%dataBlockSize != 0 {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, _ := readDataBlock(db, currentOffset)
		copy(data[size-(size%dataBlockSize):], newData)
	}
	return data
}

func readAllDataFromDbPointer(db *openDB, pointer dbPointer) []byte {
	return readFromDbPointer(db, pointer, pointer.size)
}

func serializeDbPointer(pointer dbPointer) []byte {
	res := make([]byte, DB_POINTER_SIZE)
	binary.LittleEndian.PutUint32(res, pointer.offset)
	binary.LittleEndian.PutUint32(res[4:], pointer.size)
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

func getDbPointer(db *openDB, pointerOffset uint32) dbPointer {
	pointerData := readFromDB(db, DB_POINTER_SIZE, pointerOffset)
	return deserializeDbPointer(pointerData)
}

func getMutableDbPointer(db *openDB, pointerOffset uint32) mutableDbPointer {
	pointer := getDbPointer(db, pointerOffset)
	return mutableDbPointer{pointer: pointer, location: int64(pointerOffset)}
}

// pointerOffset is the offset of the data block pointer in the file
// returns the absolute offset in the file in which the write has ended.
func appendDataToDataBlock(db *openDB, newData []byte, pointerOffset uint32) int64 {
	db.f.Seek(int64(pointerOffset), 0)
	pointerData := make([]byte, DB_POINTER_SIZE)
	sizeRead, err := db.f.Read(pointerData)
	check(err)
	assert(sizeRead == int(DB_POINTER_SIZE), "Failed to read db pointer")
	previousPointer := deserializeDbPointer(pointerData)
	newSize := previousPointer.size + uint32(len(newData))
	newSizeData := make([]byte, 4)
	binary.LittleEndian.PutUint32(newSizeData, newSize)

	// Write new pointer size
	db.f.Seek(int64(pointerOffset)+4, 0) // go the location of the pointer's size
	db.f.Write(newSizeData)

	// Write new data to data block
	// TODO: allow extending to new data blocks if necessary
	// (currently a bug will occur when the data block reaches its maximal block size)
	db.f.Seek(int64(previousPointer.offset)+int64(previousPointer.size), 0)
	db.f.Write(newData)
	finalOffset, err := db.f.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	return finalOffset
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
