package go_db

import (
	"fmt"
	"sort"
	"strings"
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

type conditionInterval struct {
	start    uint32
	end      uint32
	operator string
}

type conditionStrings struct {
	firstOperand  string
	secondOperand string
	operator      string
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

func parseCondition(condStrings conditionStrings, scheme tableScheme,
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

func parseConditionString(whereStatement string, condInterval conditionInterval,
	scheme tableScheme, nameToIndex map[string]uint32) (*condition, error) {
	conditionString := whereStatement[condInterval.start : condInterval.end+1]

	// Remove parentheses
	if len(conditionString) < 2 {
		return nil, fmt.Errorf("invalid condition %s", conditionString)
	}
	conditionString = conditionString[1 : len(conditionString)-1]

	operatorIndex := strings.Index(conditionString, condInterval.operator)
	if operatorIndex == -1 {
		return nil, fmt.Errorf("failed to find operator %s in condition %s",
			condInterval.operator, conditionString)
	}
	firstOperand := conditionString[:operatorIndex]
	secondOperand := conditionString[operatorIndex+1:]
	firstOperand = removeWhitespaces(firstOperand)
	secondOperand = removeWhitespaces(secondOperand)
	return parseCondition(conditionStrings{firstOperand: firstOperand, secondOperand: secondOperand,
		operator: condInterval.operator}, scheme, nameToIndex)
}

func getConditionInterval(conditionString string, index uint32, operator string) (*conditionInterval, error) {
	start := -1
	for i := int(index) - 1; i >= 0; i-- {
		if conditionString[i] == '(' {
			start = i
			break
		}
	}
	endOffset := strings.Index(conditionString[index+1:], ")")
	if start == -1 || endOffset == -1 {
		return nil, fmt.Errorf("failed to find matching parentheses around condition in index %d",
			index)
	}
	end := uint32(endOffset) + 1 + index
	return &conditionInterval{start: uint32(start), end: end, operator: operator}, nil
}

func doIntervalsOverlap(intervals []conditionInterval) bool {
	intervalsStartComparison := func(i, j int) bool {
		return intervals[i].start < intervals[j].start
	}

	sort.SliceStable(intervals, intervalsStartComparison)
	for i := 0; i < len(intervals)-1; i++ {
		if intervals[i].end >= intervals[i+1].start {
			return true
		}
	}
	return false
}

func parseSelectWhere(sql string, nameToIndex map[string]uint32, scheme tableScheme) {
	whereIndex := strings.Index(sql, "where")
	if whereIndex == -1 {
		return
	}

	// TODO: handle queries with something after the condition
	whereStatement := sql[whereIndex+len("where"):]

	// Find all the conditions in the "where" statement
	conditionOperatorIndexes := map[uint32]string{}
	for operator := range CONDITION_OPERATORS {
		for i := 0; i < len(whereStatement); i++ {
			if whereStatement[i:i+len(operator)] == operator {
				conditionOperatorIndexes[uint32(i)] = operator
			}
		}
	}
	conditionRanges := make([]conditionInterval, 0)
	for index, operator := range conditionOperatorIndexes {
		interval, err := getConditionInterval(whereStatement, index, operator)
		if err != nil {
			return
		}
		conditionRanges = append(conditionRanges, *interval)
	}

	// Make sure the condition ranges do not overlap
	if doIntervalsOverlap(conditionRanges) {
		return
	}

	parsedConditions := make([]condition, len(conditionRanges))
	for i := 0; i < len(conditionRanges); i++ {
		parsedCondition, err := parseConditionString(whereStatement, conditionRanges[i],
			scheme, nameToIndex)
		if err != nil {
			return
		}
		parsedConditions[i] = *parsedCondition
	}
	return
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
	parseSelectWhere(sql, nameToIndex, tableHeaders.scheme)
	condition := conditionNode{}

	return &selectQuery{columns: columnIndexes, tableID: tableID, condition: condition}, nil
}
