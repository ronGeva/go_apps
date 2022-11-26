/*
This module contains logic related to a record inside a table.
A record represents a single "row" of a relational DB table.
*/
package go_db

type Record struct {
	fields []Field
}

func MakeRecord(fields []Field) Record {
	return Record{fields: fields}
}

// Serializes the record.
// Writes remote data of db pointers as well
func serializeRecord(openDatabse *openDB, record Record) []byte {
	recordData := make([]byte, 0)
	for _, field := range record.fields {
		fieldData := field.serialize()
		if len(fieldData) <= 4 {
			// field's data is small enough to be contained locally in a DB pointer
			recordData = append(recordData, uint32ToBytes(0)...) // offset == 0, to represent a null pointer
			recordData = append(recordData, fieldData...)
		} else {
			pointer := allocateNewDataBlock(openDatabse)
			writeToDataBlock(openDatabse, pointer, recordData, 0)
			pointer.size = uint32(len(recordData))
			recordData = append(recordData, serializeDbPointer(pointer)...)
		}
	}
	return recordData
}
