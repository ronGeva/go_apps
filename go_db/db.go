package go_db

import (
	"encoding/binary"
	"fmt"
	"os"
)

// LOCAL DB
/*
Structure of a local DB:
<MAGIC> - 4 bytes
<size of data block> - 4 bytes
<pointer to free blocks bitmap>
<pointer to tables array>
<provenance on> - 1 byte (true/false)

Structure of db pointer:
<offset in the file of data block> - 4 bytes
<total size of data (in all stringed data blocks combined)> - 4 bytes
A pointer with offset < DATA_BLOCK_SIZE holds the data itself in the size field.
The value of the offset in this case holds the size of the data (0 to 4 bytes).

Structure of free blocks bitmap:
<bits> - <size of bitmap>
Each bit in the free blocks bitmap represents whether the i-th data block in the db file is free
to use.

Structure of tables array:
<tables array> - <db pointer size> * <size>, each entry is a db pointer

Structure of table:
<size of unique ID> - 4 bytes
<unique id> - variable size
<scheme> - variable size
<table bitmap pointer> - <db pointer size>
<records array pointer> - <db pointer size>

Structure of table scheme:
<size of column headers> - 4 bytes
<columns headers> - variable length
column headers appear consecutively on disk one after the other.
Note that some of the column headers might be of Provenance columns, which are stored identically on disk,
but are used differently in the Record object.

Structure of column header:
<column type> - 1 byte
<size of column name> - 4 bytes
<column name> - variable length
<index pointer> - a pointer to a B+ tree represnting the index of this column

Structure of table bitmap
<bits> - <size of bitmap>
Each bit represents whether the i-th record in the table is valid or not.
Deleted records become invalid.
Future inserted records are only added as a new record if there are no
invalid records in the table.

Structure of records array:
<records array> - <db pointer size> * <records table size>
Each cell in the records array contains a DB pointer describing the record.

Data blocks structure:
A data block is a sequence of <data block size> - 4 bytes of data, whose structure is defined
by the element whose data this block contains.
Following the data is a 4 byte value representing the offset in the file of the next data block,
which allows us to hold an arbitrary long sequence of bytes for every db structure.
*/

const LOCAL_DB_CONST uint32 = 0x1414ffbc
const DB_POINTER_SIZE uint32 = 8
const DATA_BLOCK_SIZE_SIZE uint32 = 4
const PROVENANCE_DATA_SIZE uint32 = 1
const LOCAL_DB_CONST_SIZE = 4
const DATABLOCK_BITMAP_POINTER_OFFSET = LOCAL_DB_CONST_SIZE + DATA_BLOCK_SIZE_SIZE
const TABLES_POINTER_OFFSET = DATABLOCK_BITMAP_POINTER_OFFSET + DB_POINTER_SIZE

// According to the description of the DB header
const DB_HEADER_SIZE = LOCAL_DB_CONST_SIZE + PROVENANCE_DATA_SIZE + DATA_BLOCK_SIZE_SIZE + 2*DB_POINTER_SIZE

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

	i := 0
	magic := binary.LittleEndian.Uint32(data[i:LOCAL_DB_CONST_SIZE])
	if magic != LOCAL_DB_CONST {
		panic(MalformedDBError{})
	}
	i += LOCAL_DB_CONST_SIZE

	dataBlockSize := binary.LittleEndian.Uint32(data[i : i+int(DATA_BLOCK_SIZE_SIZE)])
	i += int(DATA_BLOCK_SIZE_SIZE)

	bitmapPointer := deserializeDbPointer(data[i : i+int(DB_POINTER_SIZE)])
	i += int(DB_POINTER_SIZE)

	tablesPointer := deserializeDbPointer(data[i : i+int(DB_POINTER_SIZE)])
	i += int(DB_POINTER_SIZE)

	provenanceByte := data[i : i+1]
	provenanceOn := true
	if provenanceByte[0] == 0 {
		provenanceOn = false
	}
	i += 1

	return dbHeader{magic: magic, dataBlockSize: dataBlockSize,
		bitmapPointer: bitmapPointer, tablesPointer: tablesPointer, provenanceOn: provenanceOn}
}

func InitializeDB(path string, provenanceOn bool) {
	f := getIOInterface(path, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	// this is a bug workaround, file is always created as read-only, change it to read-write
	defer closeDBFile(f, 0600)

	// Note that a few data blocks must be reserved for the header, bitmap, and the tables array
	// 3 blocks are necessary
	dataBlockSize := 1024 // TODO: make dynamic
	f.Write(make([]byte, dataBlockSize*3))
	f.Seek(0, 0)

	// we're already using the first 4 bytes in the bitmap
	bitmapPointer := dbPointer{offset: uint32(dataBlockSize), size: 4}
	tablePointer := dbPointer{offset: 2 * uint32(dataBlockSize), size: 0}

	dbMagicBytes := uint32ToBytes(LOCAL_DB_CONST)
	dataBlockSizeBytes := uint32ToBytes(uint32(dataBlockSize))
	f.Write(dbMagicBytes)
	f.Write(dataBlockSizeBytes)
	f.Write(serializeDbPointer(bitmapPointer))
	f.Write(serializeDbPointer(tablePointer))
	if provenanceOn {
		f.Write([]byte{byte(1)})
	} else {
		f.Write([]byte{byte(0)})
	}

	// write bitmap
	f.Seek(int64(dataBlockSize), 0)
	f.Write(uint32ToBytes(7)) // first three blocks are taken

	// write tables array
	// Nothing to write - table array is initialized as empty
}

func getIOInterface(dbPath string, mode int) IoInterface {
	var f IoInterface = nil
	var err error = nil

	if dbPath == IN_MEMORY_BUFFER_PATH_MAGIC {
		if IN_MEMORY_BUFFER == nil {
			IN_MEMORY_BUFFER, err = createInMemoryBuffer(1024 * 1024 * 100) // 100 MB
		}
		f = IN_MEMORY_BUFFER
	} else {
		// TODO: change this to allow multiple read-writes at the same time
		f, err = os.OpenFile(dbPath, mode, os.ModeExclusive)
	}
	check(err)

	return f
}

func getOpenDB(db database, prov *DBProvenance) (*openDB, error) {
	f := getIOInterface(db.id.identifyingString, os.O_RDWR)

	headerData := readFromFile(f, DB_HEADER_SIZE, 0)
	header := deserializeDbHeader(headerData)

	provOn := header.provenanceOn
	provSupplied := prov != nil
	if provOn != provSupplied {
		return nil, fmt.Errorf("db provenance fields do not much provenance supplied: %t, %t", provOn, provSupplied)
	}

	// create the basic openDB object with empty authentication
	openDb := openDB{db: db, f: f, header: header,
		authentication: ProvenanceAuthentication{User: "", Password: ""},
		connection:     ProvenanceConnection{Ipv4: 0}}
	if prov != nil {
		openDb.authentication = prov.Auth
		openDb.connection = prov.Conn
		if prov.Settings != nil {
			openDb.provSettings = *prov.Settings
		} else {
			openDb.provSettings = DEFAULT_PROVENANCE_SETTINGS
		}
		openDb.provFields = generateOpenDBProvenance(&openDb)
	}

	return &openDb, nil
}

func closeOpenDB(db *openDB) {
	db.f.Close()
}

func GenerateDBUniqueID(identifier string) databaseUniqueID {
	// TODO: validate identifier is valid, check DB type
	return databaseUniqueID{ioType: LocalFile, identifyingString: identifier}
}
