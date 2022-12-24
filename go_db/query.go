package go_db

import (
	"fmt"
	"strings"
)

type queryType int8

const (
	QueryTypeInvalid queryType = iota
	QueryTypeSelect
)

type stringSet map[string]interface{}
type uint32Set map[uint32]interface{}

type selectQuery struct {
	// The columns asked to retrieve
	columns uint32Set
	// The table the query should be performed on
	// TODO: support JOINs(?)
	tableID string
	// The conditions relevant for this query
	condition conditionNode
}

type parenthesesInterval struct {
	start       int
	end         int
	parentheses bool
}

type conditionInterval struct {
	start int
	end   int
}

type conditionStrings struct {
	firstOperand  string
	secondOperand string
	operator      string
}

type OperatorDescriptor struct {
	str      string
	operator conditionOperator
}

var QUERY_TYPE_MAP = map[string]queryType{
	"select": QueryTypeSelect,
}

var LOGICAL_OPREATORS = []OperatorDescriptor{
	{"or", ConditionOperatorOr},
	{"and", ConditionOperatorAnd},
	{"not", ConditionOperatorNot},
}

var CONDITION_OPERATORS = map[string]conditionType{
	"=": ConditionTypeEqual,
	">": ConditionTypeGreater,
	"<": ConditionTypeLess}

func indexOfWord(words []string, search string) int {
	for i := 0; i < len(words); i++ {
		if words[i] == search {
			return i
		}
	}
	return len(words)
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

func nullifyWhitespaceCharacter(r rune) rune {
	if isWhitespace(r) {
		return -1
	}
	return r
}

func removeWhitespaces(str string) string {
	return strings.Map(nullifyWhitespaceCharacter, str)
}

func tableIDFromQuery(words []string) string {
	tableIDIndex := len(words) // set the index as something illegal
	for index := range words {
		if words[index] == "from" {
			tableIDIndex = index + 1
			break
		}
	}
	if tableIDIndex >= len(words) {
		return ""
	}
	return words[tableIDIndex]
}

func columnNamesFromQuery(words []string) (stringSet, error) {
	if len(words) == 0 {
		return nil, fmt.Errorf("cannot extract columns from empty query")
	}
	if !strings.EqualFold(words[0], "select") {
		return nil, fmt.Errorf("cannot extract columns from a non select query")
	}

	fromIndex := indexOfWord(words, "from")
	if fromIndex == len(words) {
		return nil, fmt.Errorf("failed to find 'from' clause in query, query parse has failed")
	}

	// Column names are between the 'select' and the 'from' clauses
	columnNames := stringSet{}
	for i := 1; i < fromIndex; i++ {
		// Column names should be separated by a comma (',')
		if i < fromIndex-1 {
			if words[i][len(words[i])-1] != ',' {
				return nil, fmt.Errorf("expected ',' after column name %s", words[i])
			}
			words[i] = words[i][:len(words[i])-1]
		}
		columnNames[words[i]] = struct{}{}
	}

	return columnNames, nil
}

func tableColumnNameToIndex(scheme tableScheme) map[string]uint32 {
	nameToIndex := map[string]uint32{}
	for i, column := range scheme.columns {
		nameToIndex[strings.ToLower(column.columnName)] = uint32(i)
	}
	return nameToIndex
}

func columnNamesToColumnIndexes(scheme tableScheme, nameToIndex map[string]uint32,
	columnNames stringSet) (uint32Set, error) {
	columnIndexes := uint32Set{}
	for columnName := range columnNames {
		if index, exists := nameToIndex[columnName]; exists {
			columnIndexes[index] = struct{}{}
		} else {
			return nil, fmt.Errorf("no matching column %s in table", columnName)
		}
	}
	return columnIndexes, nil
}

func parseSingleConditionInternal(condStrings conditionStrings, scheme tableScheme,
	nameToIndex map[string]uint32) (*condition, error) {
	// We currently assume the first operand always refer to a column name while the
	// second operand always refer to a value

	// TODO: handle column-column comparison, and conditions in which the value is on
	// the left operand
	index, exists := nameToIndex[condStrings.firstOperand]
	if !exists {
		return nil, fmt.Errorf("condition contains non-existing column %s", condStrings.firstOperand)
	}
	firstColumn := scheme.columns[index]
	parseFunc := FIELD_TYPE_QUERY_VALUE_PARSE[firstColumn.columnType]
	value, err := parseFunc(condStrings.secondOperand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse compared value %s of column %s",
			condStrings.secondOperand, condStrings.firstOperand)
	}
	cond := condition{fieldIndex: index,
		conditionType:  CONDITION_OPERATORS[condStrings.operator],
		conditionValue: value}
	return &cond, nil
}

func findMatchingClosingParentheses(sql string, i int, end int) (int, uint32) {
	count := 1
	i++
	for i < end && count > 0 {
		if sql[i] == '(' {
			count++
		}
		if sql[i] == ')' {
			count--
		}
		if count == 0 {
			break
		}
		i++
	}
	return i, uint32(count)
}

func divideStatementByParentheses(sql string, start int, end int) ([]parenthesesInterval, error) {
	intervals := []parenthesesInterval{}
	currentInterval := parenthesesInterval{start: start, parentheses: false}
	count := uint32(0)

	for i := start; i < end; i++ {
		if sql[i] == '(' {
			// Handle the edge case where the statement begins with '('
			if i > start {
				currentInterval.end = i - 1
				intervals = append(intervals, currentInterval)
			}
			currentInterval = parenthesesInterval{start: i + 1, parentheses: true}

			i, count = findMatchingClosingParentheses(sql, i, end)

			if count != 0 {
				return nil, fmt.Errorf("invalid parentheses")
			}
			currentInterval.end = i
			intervals = append(intervals, currentInterval)
			currentInterval = parenthesesInterval{start: i + 1, parentheses: false}
		}
	}
	currentInterval.end = end
	if currentInterval.start < currentInterval.end {
		intervals = append(intervals, currentInterval)
	}

	return intervals, nil
}

func parseSingleCondition(sql string, scheme tableScheme, nameToIndex map[string]uint32,
	start int, end int) (*conditionNode, error) {
	for i := start; i < end; i++ {
		for operator := range CONDITION_OPERATORS {
			if sql[i:i+len(operator)] == operator {
				firstOperand := removeWhitespaces(sql[start:i])
				secondOperand := removeWhitespaces(sql[i+len(operator) : end])
				condStrings := conditionStrings{firstOperand: firstOperand,
					secondOperand: secondOperand, operator: operator}
				cond, err := parseSingleConditionInternal(condStrings, scheme, nameToIndex)
				if err != nil {
					return nil, err
				}
				return &conditionNode{condition: cond}, nil
			}
		}
	}
	return nil, fmt.Errorf("failed to parse single condition %s", sql[start:end])
}

func divideStatementByOperator(sql string, paranthesesIntervals []parenthesesInterval, operator string,
	start int, end int) ([]conditionInterval, error) {
	intervals := make([]conditionInterval, 0)
	currInterval := conditionInterval{start: start}
	for _, interval := range paranthesesIntervals {
		if interval.end <= start || interval.start > end || interval.parentheses {
			continue
		}
		for i := max(int(interval.start), start); i < min(int(interval.end), end)-len(operator)+1; i++ {
			if sql[i:i+len(operator)] == operator {
				currInterval.end = i
				intervals = append(intervals, currInterval)
				currInterval = conditionInterval{start: i + len(operator)}
			}
		}
	}
	currInterval.end = end
	if currInterval.start < currInterval.end {
		intervals = append(intervals, currInterval)
	}
	return intervals, nil
}

func parseConditionByOperator(sql string, nameToIndex map[string]uint32, scheme tableScheme,
	parenthesesIntervals []parenthesesInterval, operatorIndex int, start int, end int) (*conditionNode, error) {

	// divide by or operators
	operatorSeparatedIntervals, err := divideStatementByOperator(sql, parenthesesIntervals,
		LOGICAL_OPREATORS[operatorIndex].str, start, end)
	if err != nil {
		return nil, err
	}

	node := conditionNode{operator: LOGICAL_OPREATORS[operatorIndex].operator}
	subnodes := make([]*conditionNode, 0)
	nextOperatorIndex := operatorIndex + 1
	for _, interval := range operatorSeparatedIntervals {
		var node *conditionNode
		var err error
		if nextOperatorIndex < len(LOGICAL_OPREATORS) {
			node, err = parseConditionByOperator(sql, nameToIndex, scheme, parenthesesIntervals, nextOperatorIndex,
				int(interval.start), int(interval.end))
		} else {
			node, err = parseCondition(sql, nameToIndex, scheme, int(interval.start), int(interval.end))
		}
		if err != nil {
			return nil, err
		}
		// Handle empty conditions
		if node != nil {
			subnodes = append(subnodes, node)
		}
	}

	if len(subnodes) == 0 {
		return nil, fmt.Errorf("failed to parse condition %s", sql[start:end])
	}
	if len(operatorSeparatedIntervals) == 1 {
		return subnodes[0], nil
	}

	node.operands = subnodes
	return &node, nil
}

func isAtomicCondition(sql string, start int, end int) bool {
	for _, operator := range LOGICAL_OPREATORS {
		for i := start; i < end-len(operator.str)+1; i++ {
			if sql[i:i+len(operator.str)] == operator.str {
				return false
			}
		}
	}
	return true
}

func parseCondition(sql string, nameToIndex map[string]uint32, scheme tableScheme,
	start int, end int) (*conditionNode, error) {
	// ignore surronding whitespaces
	for isWhitespace(rune(sql[start])) {
		start++
	}
	for isWhitespace(rune(sql[end-1])) {
		end--
	}

	if start >= end {
		// This is not an error, merely an empty condition
		return nil, nil
	}

	var node *conditionNode
	var err error
	// Find all substrings held together by parentheses
	parenthesesIntervals, err := divideStatementByParentheses(sql, start, end)
	if err != nil {
		return nil, err
	}

	if len(parenthesesIntervals) == 1 && parenthesesIntervals[0].parentheses {
		node, err = parseCondition(sql, nameToIndex, scheme,
			parenthesesIntervals[0].start, parenthesesIntervals[0].end)
	} else {
		if len(parenthesesIntervals) == 1 && isAtomicCondition(sql, parenthesesIntervals[0].start,
			parenthesesIntervals[0].end) {
			node, err = parseSingleCondition(sql, scheme, nameToIndex,
				parenthesesIntervals[0].start, parenthesesIntervals[0].end)
		} else {
			node, err = parseConditionByOperator(sql, nameToIndex, scheme, parenthesesIntervals, 0,
				start, end)
		}
	}

	if err != nil {
		return nil, err
	}
	return node, nil
}

func parseSelectWhere(sql string, nameToIndex map[string]uint32,
	scheme tableScheme) (*conditionNode, error) {
	whereIndex := strings.Index(sql, "where")
	if whereIndex == -1 {
		return nil, nil
	}

	// TODO: handle queries with something after the condition
	whereStatement := sql[whereIndex+len("where"):]
	node, err := parseCondition(whereStatement, nameToIndex, scheme, 0, len(whereStatement))
	if err != nil {
		return nil, err
	}
	return node, nil
}

func parseSelectQuery(db *openDB, sql string) (*selectQuery, error) {
	sql = strings.ToLower(sql) // normalize query by lowering it
	words := strings.FieldsFunc(sql, isWhitespace)
	tableID := tableIDFromQuery(words)
	tablePointer, err := findTable(db, tableID)
	if err != nil {
		return nil, err
	}
	tableHeaders := parseTableHeaders(db, *tablePointer)
	columnNames, err := columnNamesFromQuery(words)
	if err != nil {
		return nil, err
	}
	nameToIndex := tableColumnNameToIndex(tableHeaders.scheme)
	columnIndexes, err := columnNamesToColumnIndexes(tableHeaders.scheme, nameToIndex, columnNames)
	if err != nil {
		return nil, err
	}
	cond, err := parseSelectWhere(sql, nameToIndex, tableHeaders.scheme)
	if err != nil {
		return nil, err
	}

	return &selectQuery{columns: columnIndexes, tableID: tableID, condition: *cond}, nil
}

func parseQueryType(sql string) (queryType, error) {
	sql = strings.ToLower(sql) // normalize query by lowering it
	words := strings.FieldsFunc(sql, isWhitespace)
	if len(words) == 0 {
		return QueryTypeInvalid, fmt.Errorf("empty query")
	}
	return QUERY_TYPE_MAP[words[0]], nil
}
