/*
This module contains logic related to a record inside a table.
A record represents a single "row" of a relational DB table.
*/
package go_db

import (
	"strconv"
)

type Record struct {
	Fields     []Field
	Provenance []ProvenanceField
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

	for _, provField := range record.Provenance {
		fieldData := provField.serialize()
		recordData = append(recordData, serializeField(openDatabse, fieldData)...)
	}
	return recordData
}

func getRecordData(db *openDB, recordData []byte) []byte {
	pointer := deserializeDbPointer(recordData)
	return readAllDataFromDbPointer(db, pointer)
}

func deserializeRecordColumns(db *openDB, recordData []byte, headers []columnHeader) []Field {
	fields := make([]Field, 0)
	for i := 0; i < len(headers); i++ {
		currPointerData := recordData[i*int(DB_POINTER_SIZE) : int((i+1))*int(DB_POINTER_SIZE)]
		currPointer := deserializeDbPointer(currPointerData)
		currData := readAllDataFromDbPointer(db, currPointer)
		deserializationFunc := FIELD_TYPE_DESERIALIZATION[headers[i].columnType]
		fields = append(fields, deserializationFunc(currData))
	}

	return fields
}

func deserializeRecord(db *openDB, recordData []byte, tableScheme tableScheme) Record {
	columnsInData := len(recordData) / int(DB_POINTER_SIZE)
	assert(columnsInData == len(tableScheme.columns)+len(tableScheme.provColumns),
		"mismatching column amount between table scheme and data given: "+
			strconv.Itoa(columnsInData)+", "+strconv.Itoa(len(tableScheme.columns)))

	fields := deserializeRecordColumns(db, recordData, tableScheme.columns)
	provFields := deserializeRecordColumns(db, recordData[len(tableScheme.columns)*int(DB_POINTER_SIZE):],
		tableScheme.provColumns)
	downcastProvFields := make([]ProvenanceField, 0)
	for _, provField := range provFields {
		downcastField, ok := provField.(ProvenanceField)
		assert(ok, "failed to downcast provenance field")
		downcastProvFields = append(downcastProvFields, downcastField)
	}

	return Record{Fields: fields, Provenance: downcastProvFields}
}

func recordsAreEqual(record1 Record, record2 Record) bool {
	if len(record1.Fields) != len(record2.Fields) {
		return false
	}
	for i := 0; i < len(record1.Fields); i++ {
		if !fieldsAreEqual(record1.Fields[i], record2.Fields[i]) {
			return false
		}
	}
	return true
}

func addRecordNoDuplications(uniqueRecords []Record, record Record) []Record {
	for _, existingRecord := range uniqueRecords {
		if recordsAreEqual(existingRecord, record) {
			return uniqueRecords
		}
	}

	return append(uniqueRecords, record)
}

// given a list of records, remove all duplications within it
func removeRecordDuplications(records []Record) []Record {
	uniqueRecords := make([]Record, 0)
	for _, record := range records {
		uniqueRecords = addRecordNoDuplications(uniqueRecords, record)
	}

	return uniqueRecords
}
