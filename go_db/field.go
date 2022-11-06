/*
This module contains logic related to a single field of a record.
*/
package go_db

type FieldType int8

const (
	FieldTypeInt FieldType = iota
	FieldTypeString
	FieldTypeBlob
)
