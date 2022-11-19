package go_db

import (
	"encoding/binary"
	"os"
)

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
<size of unique ID> - 4 bytes
<unique id> - variable size
<scheme> - variable size
<records array> - variable size

Structure of table scheme:
<size of column headers> - 4 bytes
<columns headers> - variable length
column headers appear consecutively on disk one after the other.

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
const TABLES_POINTER_OFFSET = BITMAP_POINTER_OFFSET + DB_POINTER_SIZE

// According to the description of the DB header
const DB_HEADER_SIZE = LOCAL_DB_CONST_SIZE + DATA_BLOCK_SIZE_SIZE + 2*DB_POINTER_SIZE

type databaseUniqueID struct {
	ioType            IOType
	identifyingString string
}

type database struct {
	id databaseUniqueID
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

// TODO: make private in the future
func InitializeDB(path string) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0)
	defer f.Close()
	check(err)

	// TODO: implement initialization of the DB
	// Note that a few data blocks must be reserved for the header, bitmap, and the tables array
	// 3 blocks are necessary
	dataBlockSize := 1024 // TODO: make dynamic
	f.Write(make([]byte, dataBlockSize*3))
	f.Seek(0, 0)

	// we're already using the first 4 bytes in the bitmap and in the tables array
	bitmapPointer := dbPointer{offset: uint32(dataBlockSize), size: 4}
	tablePointer := dbPointer{offset: 2 * uint32(dataBlockSize), size: 4}

	dbMagicBytes := uint32ToBytes(LOCAL_DB_CONST)
	dataBlockSizeBytes := uint32ToBytes(uint32(dataBlockSize))
	f.Write(dbMagicBytes)
	f.Write(dataBlockSizeBytes)
	f.Write(serializeDbPointer(bitmapPointer))
	f.Write(serializeDbPointer(tablePointer))

	// write bitmap
	f.Seek(int64(dataBlockSize), 0)
	f.Write(uint32ToBytes(7)) // first three blocks are taken
	// write tables array
	f.Seek(2*int64(dataBlockSize), 0)
	f.Write(uint32ToBytes(0))
}
