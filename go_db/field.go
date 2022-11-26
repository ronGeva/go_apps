/*
This module contains logic related to a single field of a record.
*/
package go_db

import "encoding/binary"

type FieldType int8

const (
	FieldTypeInt FieldType = iota
	FieldTypeString
	FieldTypeBlob
)

type Field interface {
	getType() FieldType
	serialize() []byte
}

type IntField struct {
	Value int
}

func (field *IntField) getType() FieldType {
	return FieldTypeInt
}

func (field *IntField) serialize() []byte {
	res := make([]byte, 4)

	binary.LittleEndian.PutUint32(res, uint32(field.Value))
	return res
}
