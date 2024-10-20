/*
This module contains logic related to a record inside a table.
A record represents a single "row" of a relational DB table.
*/
package go_db

import (
	"strconv"

	"github.com/ronGeva/go_apps/b_tree"
)

type Record struct {
	Fields     []Field
	Provenance []ProvenanceField
}

func (record *Record) getRecordKey(isProv bool, offset int) *b_tree.BTreeKeyType {
	if isProv {
		return record.Provenance[offset].ToKey()
	} else {
		return record.Fields[offset].ToKey()
	}
}

func MakeRecord(fields []Field) Record {
	return Record{Fields: fields}
}

func MakeEmptyRecord() Record {
	return Record{}
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
	provData := recordData[len(tableScheme.columns)*int(DB_POINTER_SIZE):]
	provFields := provenanceDeserializeRecordProvenanceFields(db, provData, &tableScheme)
	return Record{Fields: fields, Provenance: provFields}
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

func uniqueRecordsFromIdenticalRecords(identicalRecords [][]Record) []Record {
	uniqueRecords := make([]Record, 0)
	for _, sameRecords := range identicalRecords {
		projectedRecord := provenanceApplySelect(sameRecords)
		uniqueRecords = append(uniqueRecords, projectedRecord)
	}

	return uniqueRecords
}

func addRecordToSameKeyRecords(record Record, sameKeyRecords [][]Record) [][]Record {
	for i := range sameKeyRecords {
		if recordsAreEqual(record, sameKeyRecords[i][0]) {
			sameKeyRecords[i] = append(sameKeyRecords[i], record)
			return sameKeyRecords
		}
	}

	sameKeyRecords = append(sameKeyRecords, []Record{record})
	return sameKeyRecords
}

func addRecordToSeenRecords(record Record, seenRecords map[string][][]Record, key string) {
	val, ok := seenRecords[key]
	if !ok {
		seenRecords[key] = [][]Record{{record}}
	} else {
		seenRecords[key] = addRecordToSameKeyRecords(record, val)
	}
}

func removeRecordDuplications(records []Record) []Record {
	seenRecords := make(map[string][][]Record)
	allUniqueRecords := make([]Record, 0)
	for _, record := range records {
		key := ""
		for _, field := range record.Fields {
			key += field.Stringify()
		}

		addRecordToSeenRecords(record, seenRecords, key)
	}

	for key := range seenRecords {
		sameKeyRecords := seenRecords[key]
		uniqueRecords := uniqueRecordsFromIdenticalRecords(sameKeyRecords)
		allUniqueRecords = append(allUniqueRecords, uniqueRecords...)
	}

	return allUniqueRecords
}
