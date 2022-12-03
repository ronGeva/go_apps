/*
This module contains logic related to a single field of a record.
*/
package go_db

import (
	"encoding/binary"
	"strconv"
)

type FieldType int8

const (
	FieldTypeInt FieldType = iota
	FieldTypeString
	FieldTypeBlob
)

var FIELD_TYPE_SERIALIZATION = map[FieldType]func([]byte) Field{
	FieldTypeInt:  deserializeIntField,
	FieldTypeBlob: deserializeBlobField,
}

type Field interface {
	getType() FieldType
	serialize() []byte
}

type IntField struct {
	Value int
}

func (field IntField) getType() FieldType {
	return FieldTypeInt
}

func (field IntField) serialize() []byte {
	res := make([]byte, 4)

	binary.LittleEndian.PutUint32(res, uint32(field.Value))
	return res
}

func deserializeIntField(data []byte) Field {
	assert(len(data) == 4, "An invalid data length was given: "+strconv.Itoa((len(data))))

	return IntField{int(binary.LittleEndian.Uint32(data))}
}

type BlobField struct {
	Data []byte
}

func (field BlobField) getType() FieldType {
	return FieldTypeBlob
}

func (field BlobField) serialize() []byte {
	return field.Data
}

func deserializeBlobField(data []byte) Field {
	return BlobField{Data: data}
}
