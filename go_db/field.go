/*
This module contains logic related to a single field of a record.
*/
package go_db

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
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

var FIELD_TYPE_QUERY_VALUE_PARSE = map[FieldType]func(string) ([]byte, error){
	FieldTypeInt:  intQueryValueParse,
	FieldTypeBlob: blobQueryValueParse,
}

var STRING_TO_FIELD_FUNCS = map[FieldType]func(string) (Field, error){
	FieldTypeInt:  stringToIntField,
	FieldTypeBlob: stringToBlobField,
}

var FIELD_STRING_TO_TYPE = map[string]FieldType{
	"int":    FieldTypeInt,
	"string": FieldTypeString,
	"blob":   FieldTypeBlob,
}

type Field interface {
	getType() FieldType
	serialize() []byte
	Stringify() string
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

func (field IntField) Stringify() string {
	return strconv.Itoa(field.Value)
}

func deserializeIntField(data []byte) Field {
	assert(len(data) == 4, "An invalid data length was given: "+strconv.Itoa((len(data))))

	return IntField{int(binary.LittleEndian.Uint32(data))}
}

func intQueryValueParse(data string) ([]byte, error) {
	num, err := strconv.Atoi(data)
	if err != nil {
		return nil, err
	}
	return uint32ToBytes(uint32(num)), nil
}

func blobQueryValueParse(data string) ([]byte, error) {
	// Assume the string represents the hexadecimal encoding of the data
	return hex.DecodeString(data)
}

func stringToIntField(data string) (Field, error) {
	// remove whitespaces from string
	data = strings.Trim(data, " \t\r\n")
	num, err := strconv.Atoi(data)
	if err != nil {
		return nil, err
	}
	return IntField{num}, nil
}

func stringToBlobField(data string) (Field, error) {
	byteData, err := blobQueryValueParse(data)
	if err != nil {
		return nil, err
	}
	return BlobField{Data: byteData}, nil
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

func (field BlobField) Stringify() string {
	if len(field.Data) < 50 {
		// Data is small enough, we can represent it to the user
		return hex.EncodeToString(field.Data)
	}

	return "large binary data"
}

func deserializeBlobField(data []byte) Field {
	return BlobField{Data: data}
}
