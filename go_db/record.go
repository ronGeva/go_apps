/*
This module contains logic related to a record inside a table.
A record represents a single "row" of a relational DB table.
*/
package go_db

import "strconv"

type Record struct {
	Fields []Field
}

func MakeRecord(fields []Field) Record {
	return Record{Fields: fields}
}

func serializeField(openDatabase *openDB, fieldData []byte) []byte {
	data := make([]byte, 0)
	if len(fieldData) <= 4 {
		// field's data is small enough to be contained locally in a DB pointer
		// offset < DATA_BLOCK_SIZE, to represent the data is resident
		data = append(data, uint32ToBytes(uint32(len(fieldData)))...)
		data = append(data, fieldData...)
		zeroPadding := make([]byte, 4-len(fieldData))
		data = append(data, zeroPadding...)
	} else {
		pointer := allocateNewDataBlock(openDatabase)
		appendDataToDataBlockImmutablePointer(openDatabase, fieldData, pointer)
		pointer.size = uint32(len(fieldData))
		data = append(data, serializeDbPointer(pointer)...)
	}

	return data
}

// Serializes the record.
// Writes remote data of db pointers as well
func serializeRecord(openDatabse *openDB, record Record) []byte {
	recordData := make([]byte, 0)
	for _, field := range record.Fields {
		fieldData := field.serialize()
		recordData = append(recordData, serializeField(openDatabse, fieldData)...)
	}
	return recordData
}

func getRecordData(db *openDB, recordData []byte) []byte {
	pointer := deserializeDbPointer(recordData)
	return readAllDataFromDbPointer(db, pointer)
}

func deserializeRecord(db *openDB, recordData []byte, tableScheme tableScheme) Record {
	columnsInData := len(recordData) / int(DB_POINTER_SIZE)
	assert(columnsInData == len(tableScheme.columns),
		"mismatching column amount between table scheme and data given: "+
			strconv.Itoa(columnsInData)+", "+strconv.Itoa(len(tableScheme.columns)))

	fields := make([]Field, 0)
	for i := 0; i < columnsInData; i++ {
		currPointerData := recordData[i*int(DB_POINTER_SIZE) : int((i+1))*int(DB_POINTER_SIZE)]
		currPointer := deserializeDbPointer(currPointerData)
		currData := readAllDataFromDbPointer(db, currPointer)
		deserializationFunc := FIELD_TYPE_SERIALIZATION[tableScheme.columns[i].columnType]
		fields = append(fields, deserializationFunc(currData))
	}
	return Record{Fields: fields}
}
