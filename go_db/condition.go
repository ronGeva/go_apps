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

type optionalBool struct {
	value   bool
	isValid bool
}

type condition struct {
	fieldIndex     uint32
	conditionType  conditionType
	conditionValue []byte
}

type conditionNode struct {
	operator  conditionOperator
	left      *conditionNode
	right     *conditionNode
	condition *condition
}

var SUPPORTED_CONDITIONS = map[FieldType][]conditionType{
	FieldTypeInt:  {ConditionTypeEqual, ConditionTypeLess, ConditionTypeGreater},
	FieldTypeBlob: {ConditionTypeEqual},
}

var OPEATOR_FUNCS = map[conditionOperator]func(left optionalBool, right optionalBool) bool{
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

func andFunc(left optionalBool, right optionalBool) bool {
	assert(left.isValid && right.isValid, "and operator requires two operands")
	return left.value && right.value
}

func orFunc(left optionalBool, right optionalBool) bool {
	assert(left.isValid && right.isValid, "or operator requires two operands")
	return left.value || right.value
}

func notFunc(left optionalBool, right optionalBool) bool {
	assert(left.isValid && !right.isValid, "not opreator accepts exactly one operands")
	return !left.value
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
	var leftResult optionalBool
	var rightResult optionalBool
	if currentNode.left == nil && currentNode.right == nil {
		assert(currentNode.condition != nil, "Node must have either a son or a condition")
		return checkCondition(*currentNode.condition, record)
	}

	if currentNode.left != nil {
		leftResult.value = checkAllConditions(*currentNode.left, record)
		leftResult.isValid = true
	}
	if currentNode.right != nil {
		rightResult.value = checkAllConditions(*currentNode.right, record)
		rightResult.isValid = true
	}
	assert(currentNode.operator != ConditionOperatorNull, "A node with sons must have an operator")
	return OPEATOR_FUNCS[currentNode.operator](leftResult, rightResult)
}
