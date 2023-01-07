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

// Serializes the record.
// Writes remote data of db pointers as well
func serializeRecord(openDatabse *openDB, record Record) []byte {
	recordData := make([]byte, 0)
	for _, field := range record.Fields {
		fieldData := field.serialize()
		if len(fieldData) <= 4 {
			// field's data is small enough to be contained locally in a DB pointer
			recordData = append(recordData, uint32ToBytes(0)...) // offset == 0, to represent a null pointer
			recordData = append(recordData, fieldData...)
		} else {
			pointer := allocateNewDataBlock(openDatabse)
			appendDataToDataBlockImmutablePointer(openDatabse, fieldData, pointer)
			pointer.size = uint32(len(fieldData))
			recordData = append(recordData, serializeDbPointer(pointer)...)
		}
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
