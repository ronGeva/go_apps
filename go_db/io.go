package go_db

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
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

// Must conform to the interface exposed by os.File
type IoInterface interface {
	Seek(offset int64, whence int) (ret int64, err error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
}

/////////////////// in memory IO /////////////////////////////////////////////////////////
type InMemoryBuffer struct {
	buffer        []byte
	offset        int64
	validDataSize int64
}

func (buf *InMemoryBuffer) Seek(offset int64, whence int) (ret int64, err error) {
	var newOffset int64
	// Treat the offset as absolute
	if whence == io.SeekStart {
		newOffset = 0
	}
	if whence == io.SeekCurrent {
		newOffset = buf.offset
	}
	if whence == io.SeekEnd {
		newOffset = buf.validDataSize
	}
	newOffset += offset
	if newOffset < 0 || newOffset > buf.validDataSize {
		return 0, fmt.Errorf("cannot seek past end/start of buffer")
	}

	buf.offset = newOffset
	return buf.offset, nil
}

func (buf *InMemoryBuffer) Read(b []byte) (n int, err error) {
	availableSize := buf.validDataSize - buf.offset
	sizeToRead := int64(len(b))
	if sizeToRead > availableSize {
		sizeToRead = availableSize
	}

	copy(b, buf.buffer[buf.offset:buf.offset+int64(sizeToRead)])

	buf.offset += int64(sizeToRead)

	return int(sizeToRead), nil
}

func (buf *InMemoryBuffer) Write(b []byte) (n int, err error) {
	availableSize := len(buf.buffer) - int(buf.offset)
	if availableSize < len(b) {
		return 0, fmt.Errorf("not enough space in buffer to write %d bytes", len(b))
	}

	copy(buf.buffer[buf.offset:], b)
	buf.offset += int64(len(b))

	// Update valid data size, if necessary
	if buf.offset > buf.validDataSize {
		buf.validDataSize = buf.offset
	}

	return len(b), nil
}

func (buf *InMemoryBuffer) Close() error {
	// Nothing we should do here, the gc will take care of the actual data
	return nil
}

func createInMemoryBuffer(size int64) (*InMemoryBuffer, error) {
	return &InMemoryBuffer{buffer: make([]byte, size), offset: 0, validDataSize: 0}, nil
}

const IN_MEMORY_BUFFER_PATH_MAGIC = "__IN_MEMORY_BUFFER__"

var IN_MEMORY_BUFFER *InMemoryBuffer = nil

///////////////////////////////////////////////////////////////////////////////////////

type mutableDbPointer struct {
	pointer  dbPointer
	location int64
}

type dbHeader struct {
	magic         uint32
	dataBlockSize uint32
	bitmapPointer dbPointer
	tablesPointer dbPointer
	provenanceOn  bool
}

type openDB struct {
	db             database
	f              IoInterface
	header         dbHeader
	authentication ProvenanceAuthentication
	connection     ProvenanceConnection
	provFields     []ProvenanceField
	provSettings   ProvenanceSettings
}

type allocationFuncType func(*openDB) dbPointer

// DB IO utils

func uint32ToBytes(num uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, num)
	return b
}

func readFromFile(f IoInterface, size uint32, offset uint32) []byte {
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

func writeToFile(f IoInterface, data []byte, offset uint32) {
	f.Seek(int64(offset), 0)
	sizeWritten, err := f.Write(data)
	check(err)
	assert(sizeWritten == len(data), "Failed to write the size requested from the db file")
}

func writeToDB(db *openDB, data []byte, offset uint32) {
	writeToFile(db.f, data, offset)
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

// Returns the data of the block requested, as well as the offset of the next data block
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

func putVariableSizeInt(res []byte, val uint32) error {
	// Make sure res is big enough to contain val
	if uint64(math.Pow(2, float64(8*len(res)))) <= uint64(val) {
		return fmt.Errorf("cannot cast value %d to byte array of size %d",
			val, len(res))
	}

	// little endian
	for i := 0; i < len(res); i++ {
		res[i] = byte(val % 256)
		val /= 256
	}
	return nil
}

func readFromDbPointer(db *openDB, pointer dbPointer, size uint32, offset uint32) []byte {
	if pointer.offset <= 4 {
		// pointer.offset holds the size of the data in the size field
		res := make([]byte, pointer.offset)

		putVariableSizeInt(res, pointer.size)
		return res // the size contains the actual value
	}

	data := make([]byte, size)
	finalReadOffset := size + offset
	dataInBlock := db.header.dataBlockSize - 4
	currentOffset := pointer.offset
	bytesCopied := uint32(0)
	// Read all but the last data block (if it is partial)
	for i := uint32(0); i < finalReadOffset/dataInBlock; i++ {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, nextOffset := readDataBlock(db, currentOffset)
		currentOffset = nextOffset
		if offset < dataInBlock {
			copy(data[bytesCopied:], newData[offset:])
			bytesCopied += uint32(len(newData[offset:]))
			offset = 0
		} else {
			offset -= dataInBlock
		}
	}

	// Read the last partial data block (if there is one)
	if size%dataInBlock != 0 {
		if currentOffset == 0 {
			panic(BadReadRequestError{})
		}

		newData, _ := readDataBlock(db, currentOffset)
		copy(data[bytesCopied:], newData[offset:])
	}
	return data
}

func readAllDataFromDbPointer(db *openDB, pointer dbPointer) []byte {
	return readFromDbPointer(db, pointer, pointer.size, 0)
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

func getPointerFinalBlockOffset(db *openDB, pointer dbPointer, offset *uint32) uint32 {
	currentBlockOffset := pointer.offset
	for *offset > db.header.dataBlockSize-4 {
		_, currentBlockOffset = readDataBlock(db, currentBlockOffset)
		*offset -= (db.header.dataBlockSize - 4)
	}
	return currentBlockOffset
}

// Appends the data to the data block without changing the pointer
// Can be used when another component is responsible for changing the pointer's
// value, such as when the pointer is only in memory and isn't present on disk.
func appendDataToDataBlockImmutablePointerInternal(db *openDB, newData []byte, pointer dbPointer,
	allocationFunc allocationFuncType) int64 {
	offset := pointer.size
	finalBlockOffset := getPointerFinalBlockOffset(db, pointer, &offset)
	bytesWritten := 0

	bytesToWrite := min(int(db.header.dataBlockSize-offset-4), len(newData))
	if bytesToWrite > 0 {
		writeToDB(db, newData[bytesWritten:bytesWritten+int(bytesToWrite)], finalBlockOffset+offset)
		bytesWritten += int(bytesToWrite)
	}

	for bytesWritten < len(newData) {
		// Allocate a new block
		newBlockPointer := allocationFunc(db)
		// Write its offset to the end of the current block
		writeToDB(db, uint32ToBytes(newBlockPointer.offset), finalBlockOffset+db.header.dataBlockSize-4)
		// Advance to this new block and continue writing
		finalBlockOffset = newBlockPointer.offset
		bytesToWrite := min(int(db.header.dataBlockSize-4), len(newData)-bytesWritten)
		writeToDB(db, newData[bytesWritten:bytesWritten+bytesToWrite], finalBlockOffset)
		bytesWritten += bytesToWrite
	}

	finalOffset, err := db.f.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	return finalOffset
}

func appendDataToDataBlockImmutablePointer(db *openDB, newData []byte, pointer dbPointer) int64 {
	return appendDataToDataBlockImmutablePointerInternal(db, newData, pointer, allocateNewDataBlock)
}

// pointerOffset is the offset of the data block pointer in the file
// returns the absolute offset in the file in which the write has ended.
func appendDataToDataBlockInternal(db *openDB, newData []byte, pointerOffset uint32,
	allocationFunc allocationFuncType) int64 {
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

	return appendDataToDataBlockImmutablePointerInternal(db, newData, previousPointer, allocationFunc)
}

func appendDataToBlockBitmap(db *openDB, newData []byte) int64 {
	return appendDataToDataBlockInternal(db, newData, DATABLOCK_BITMAP_POINTER_OFFSET, allocateNewDataEndOfDB)
}

func appendDataToDataBlock(db *openDB, newData []byte, pointerOffset uint32) int64 {
	return appendDataToDataBlockInternal(db, newData, pointerOffset, allocateNewDataBlock)
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func writeToDataBlock(db *openDB, pointer dbPointer, data []byte, offset uint32) {
	assert(offset+uint32(len(data)) <= pointer.size, "Cannot write passed end of data block")
	currentBlockOffset := getPointerFinalBlockOffset(db, pointer, &offset)
	offsetInWriteBuffer := 0
	for offsetInWriteBuffer < len(data) {
		db.f.Seek(int64(currentBlockOffset)+int64(offset), 0)
		sizeToWrite := min(int(db.header.dataBlockSize-4-offset), len(data)-offsetInWriteBuffer)
		offset = 0 // from now on we will always write to the start of the blocks

		n, err := db.f.Write(data[offsetInWriteBuffer : offsetInWriteBuffer+int(sizeToWrite)])
		check(err)
		assert(n == int(sizeToWrite), "")
		offsetInWriteBuffer += int(sizeToWrite)
		_, currentBlockOffset = readDataBlock(db, currentBlockOffset)
	}
}

func closeDBFile(f IoInterface, newMode int) {
	f.Close()

	file, ok := f.(*os.File)
	if ok {
		os.Chmod(file.Name(), os.FileMode(newMode))
	}
}
