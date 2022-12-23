package go_db

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type conditionType int8
type conditionOperator int8

const (
	ConditionTypeEqual conditionType = iota
	ConditionTypeLess
	ConditionTypeGreater
	ConditionTypeRegex
)

const (
	ConditionOperatorNull conditionOperator = iota
	ConditionOperatorAnd
	ConditionOperatorOr
	ConditionOperatorNot
)

type condition struct {
	fieldIndex     uint32
	conditionType  conditionType
	conditionValue []byte
}

type conditionNode struct {
	operator  conditionOperator
	operands  []*conditionNode
	condition *condition
}

var SUPPORTED_CONDITIONS = map[FieldType][]conditionType{
	FieldTypeInt:  {ConditionTypeEqual, ConditionTypeLess, ConditionTypeGreater},
	FieldTypeBlob: {ConditionTypeEqual},
}

var OPEATOR_FUNCS = map[conditionOperator]func(values []bool) bool{
	ConditionOperatorAnd: andFunc,
	ConditionOperatorOr:  orFunc,
	ConditionOperatorNot: notFunc,
}

var CONDITION_FUNCS = map[conditionType]func(f Field, data []byte) (bool, error){
	ConditionTypeEqual:   checkEqual,
	ConditionTypeLess:    checkLess,
	ConditionTypeGreater: checkGreater,
}

func isConditionSupported(scheme tableScheme, cond *condition) bool {
	supportedConditions := SUPPORTED_CONDITIONS[scheme.columns[cond.fieldIndex].columnType]
	for _, supportedCondition := range supportedConditions {
		if supportedCondition == cond.conditionType {
			return true
		}
	}
	return false
}

func andFunc(values []bool) bool {
	res := true
	for _, val := range values {
		res = res && val
	}
	return res
}

func orFunc(values []bool) bool {
	res := false
	for _, val := range values {
		res = res || val
	}
	return res
}

func notFunc(values []bool) bool {
	assert(len(values) == 1, "not opreator accepts exactly one operands")
	return !values[0]
}

func checkEqual(f Field, data []byte) (bool, error) {
	switch f.getType() {
	case FieldTypeInt:
		intField, ok := f.(IntField)
		assert(ok, "Failed to downcast IntField")
		return intField.Value == int(binary.LittleEndian.Uint32(data)), nil
	case FieldTypeBlob:
		blobField, ok := f.(BlobField)
		assert(ok, "Failed to downcase BlobField")
		return bytes.Equal(blobField.Data, data), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkLess(f Field, data []byte) (bool, error) {
	switch f.getType() {
	case FieldTypeInt:
		intField, ok := f.(IntField)
		assert(ok, "Failed to downcast IntField")
		return intField.Value < int(binary.LittleEndian.Uint32(data)), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkGreater(f Field, data []byte) (bool, error) {
	switch f.getType() {
	case FieldTypeInt:
		intField, ok := f.(IntField)
		assert(ok, "Failed to downcast IntField")
		return intField.Value > int(binary.LittleEndian.Uint32(data)), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkCondition(c condition, record Record) bool {
	field := record.fields[c.fieldIndex]
	conditionFunc := CONDITION_FUNCS[c.conditionType]
	result, err := conditionFunc(field, c.conditionValue)
	check(err) // TODO: handle error
	return result
}

func checkAllConditions(currentNode conditionNode, record Record) bool {
	values := make([]bool, 0)
	if len(currentNode.operands) == 0 {
		return checkCondition(*currentNode.condition, record)
	}

	for _, operand := range currentNode.operands {
		values = append(values, checkAllConditions(*operand, record))
	}
	return OPEATOR_FUNCS[currentNode.operator](values)
}
