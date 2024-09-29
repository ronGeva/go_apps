/*
This module contains logic related to a single field of a record.
*/
package go_db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/ronGeva/go_apps/b_tree"
)

type FieldType int8

const (
	FieldTypeInt FieldType = iota
	FieldTypeString
	FieldTypeBlob
	FieldTypeProvenance
)

var FIELD_TYPE_DESERIALIZATION = map[FieldType]func([]byte) Field{
	FieldTypeInt:        deserializeIntField,
	FieldTypeBlob:       deserializeBlobField,
	FieldTypeString:     deserializeStringField,
	FieldTypeProvenance: deserializeProvenanceField,
}

var FIELD_TYPE_QUERY_VALUE_PARSE = map[FieldType]func(string) ([]byte, error){
	FieldTypeInt:    intQueryValueParse,
	FieldTypeBlob:   blobQueryValueParse,
	FieldTypeString: stringQueryValueParse,
}

var STRING_TO_FIELD_FUNCS = map[FieldType]func(string) (Field, error){
	FieldTypeInt:    stringToIntField,
	FieldTypeBlob:   stringToBlobField,
	FieldTypeString: stringToStringField,
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

	// used for indexing
	ToKey() *b_tree.BTreeKeyType
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

func (field IntField) ToKey() *b_tree.BTreeKeyType {
	key := b_tree.BTreeKeyType(int(field.Value))
	return &key
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

func (field BlobField) ToKey() *b_tree.BTreeKeyType {
	return nil
}

func deserializeBlobField(data []byte) Field {
	return BlobField{Data: data}
}

type StringField struct {
	Value string
}

func (field StringField) getType() FieldType {
	return FieldTypeString
}

func (field StringField) serialize() []byte {
	return []byte(field.Value)
}

func (field StringField) Stringify() string {
	return field.Value
}

func (field StringField) ToKey() *b_tree.BTreeKeyType {
	return nil
}

func deserializeStringField(data []byte) Field {
	return StringField{string(data)}
}

func stringFromStringLiteral(data string) (*string, error) {
	// Make sure the data starts and ends with a quotation marks
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return nil, fmt.Errorf("invalid string literal %s", data)
	}

	// remove quotation marks
	res := data[1 : len(data)-1]
	return &res, nil
}

func stringToStringField(data string) (Field, error) {
	str, err := stringFromStringLiteral(data)
	if err != nil {
		return nil, err
	}

	return StringField{Value: *str}, nil
}

func stringQueryValueParse(data string) ([]byte, error) {
	str, err := stringFromStringLiteral(data)
	if err != nil {
		return nil, err
	}

	return []byte(*str), nil
}

func fieldsAreEqual(field1 Field, field2 Field) bool {
	if field1.getType() != field2.getType() {
		return false
	}

	data1 := field1.serialize()
	data2 := field2.serialize()
	return bytes.Compare(data1, data2) == 0
}
