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

type operand struct {
	fieldIndex   *uint32
	valueLiteral []byte
}

func (op *operand) getInt(record *Record) int {
	if op.fieldIndex != nil {
		field, ok := record.Fields[*op.fieldIndex].(IntField)
		assert(ok, "Failed to downcast IntField")
		return field.Value
	} else {
		return int(binary.LittleEndian.Uint32(op.valueLiteral))
	}
}

func (op *operand) getString(record *Record) string {
	if op.fieldIndex != nil {
		field, ok := record.Fields[*op.fieldIndex].(StringField)
		assert(ok, "Failed to downcast StringField")
		return field.Value
	} else {
		return string(op.valueLiteral)
	}
}

func (op *operand) getBlob(record *Record) []byte {
	if op.fieldIndex != nil {
		field, ok := record.Fields[*op.fieldIndex].(BlobField)
		assert(ok, "Failed to downcast BlobField")
		return field.Data
	} else {
		return op.valueLiteral
	}
}

type condition struct {
	leftOperand   operand
	rightOperand  operand
	conditionType conditionType
}

func (cond *condition) valuesType(record Record) FieldType {
	if cond.leftOperand.fieldIndex != nil {
		return record.Fields[*cond.leftOperand.fieldIndex].getType()
	}

	return record.Fields[*cond.rightOperand.fieldIndex].getType()
}

type conditionNode struct {
	operator  conditionOperator
	operands  []*conditionNode
	condition *condition
}

var SUPPORTED_CONDITIONS = map[FieldType][]conditionType{
	FieldTypeInt:    {ConditionTypeEqual, ConditionTypeLess, ConditionTypeGreater},
	FieldTypeBlob:   {ConditionTypeEqual},
	FieldTypeString: {ConditionTypeEqual},
}

var OPEATOR_FUNCS = map[conditionOperator]func(values []bool) bool{
	ConditionOperatorAnd: andFunc,
	ConditionOperatorOr:  orFunc,
	ConditionOperatorNot: notFunc,
}

var CONDITION_FUNCS = map[conditionType]func(record *Record, cond condition) (bool, error){
	ConditionTypeEqual:   checkEqual,
	ConditionTypeLess:    checkLess,
	ConditionTypeGreater: checkGreater,
}

func isConditionSupported(valuesType FieldType, condType conditionType) bool {
	supportedConditions := SUPPORTED_CONDITIONS[valuesType]
	for _, supportedCondition := range supportedConditions {
		if supportedCondition == condType {
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

func checkEqual(record *Record, cond condition) (bool, error) {
	switch cond.valuesType(*record) {
	case FieldTypeInt:
		return cond.leftOperand.getInt(record) == cond.rightOperand.getInt(record), nil
	case FieldTypeBlob:
		return bytes.Equal(cond.leftOperand.getBlob(record),
			cond.rightOperand.getBlob(record)), nil
	case FieldTypeString:
		return cond.leftOperand.getString(record) == cond.rightOperand.getString(record), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkLess(record *Record, cond condition) (bool, error) {
	switch cond.valuesType(*record) {
	case FieldTypeInt:
		return cond.leftOperand.getInt(record) < cond.rightOperand.getInt(record), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkGreater(record *Record, cond condition) (bool, error) {
	switch cond.valuesType(*record) {
	case FieldTypeInt:
		return cond.leftOperand.getInt(record) > cond.rightOperand.getInt(record), nil
	default:
		return false, fmt.Errorf("illegal condition")
	}
}

func checkCondition(c condition, record Record) bool {
	conditionFunc := CONDITION_FUNCS[c.conditionType]
	result, err := conditionFunc(&record, c)
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
