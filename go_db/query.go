package go_db

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type nonExistentColumnError struct {
	Description string
}

func (e *nonExistentColumnError) Error() string {
	return e.Description
}

type queryType int8

const (
	QueryTypeInvalid queryType = iota
	QueryTypeSelect
	QueryTypeInsert
	QueryTypeDelete
	QueryTypeCreate
	QueryTypeUpdate
)

type columnNameType int8

const (
	columnNameTypeUnknown columnNameType = iota
	columnNamesTypeSingleTable
	columnNamesTypeMultipleTables
)

type stringSet map[string]interface{}

type selectQuery struct {
	columnNames []string

	// The columns asked to retrieve, in the order they've been requested
	columns []uint32
	// The tables the query should be performed on
	tableIDs []string
	// The conditions relevant for this query
	condition *conditionNode

	// an optional uint32 that marks the column used to sort the result
	orderBy *uint32

	// An optional uint32 describing how many records we'd like to retrieve.
	// The records received would be the one with the best provenance score.
	bestAmount *uint32
}

type insertQuery struct {
	tableID string
	records []Record
}

type deleteQuery struct {
	tableID   string
	condition *conditionNode
}

type createQuery struct {
	tableID string
	scheme  tableScheme
}

type updateQuery struct {
	tableID   string
	condition *conditionNode
	update    recordUpdate
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
	"insert": QueryTypeInsert,
	"delete": QueryTypeDelete,
	"create": QueryTypeCreate,
	"update": QueryTypeUpdate,
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

func stringToWords(str string) []string {
	return strings.FieldsFunc(str, isWhitespace)
}

func tableIDsFromQuery(words []string, wordBefore string) []string {
	tableIDIndex := len(words) // set the index as something illegal
	for index := range words {
		if words[index] == wordBefore {
			tableIDIndex = index + 1
			break
		}
	}
	if tableIDIndex >= len(words) {
		return nil
	}
	tableIDs := []string{words[tableIDIndex]}
	tableIDIndex += 1
	// keep adding tables which appear in the format:
	// <tableID> JOIN <tableID2> JOIN <tableID3> ...
	for tableIDIndex < len(words)-1 && words[tableIDIndex] == "join" {
		tableIDs = append(tableIDs, words[tableIDIndex+1])
		tableIDIndex += 2
	}

	return tableIDs
}

func tableIDFromQuery(words []string, wordBefore string) (string, error) {
	tableIDs := tableIDsFromQuery(words, wordBefore)
	if len(tableIDs) != 1 {
		return "", fmt.Errorf("expected exactly one table name to mentioned, found %d in query",
			len(tableIDs))
	}

	return tableIDs[0], nil
}

func columnNamesFromQuery(words []string) ([]string, error) {
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
	columnNames := make([]string, 0)
	for i := 1; i < fromIndex; i++ {
		// Column names should be separated by a comma (',')
		if i < fromIndex-1 {
			if words[i][len(words[i])-1] != ',' {
				return nil, fmt.Errorf("expected ',' after column name %s", words[i])
			}
			words[i] = words[i][:len(words[i])-1]
		}
		columnNames = append(columnNames, words[i])
	}

	return columnNames, nil
}

func stringListToSet(list []string) stringSet {
	set := make(stringSet)
	for _, name := range list {
		set[name] = nil
	}
	return set
}

// the tablePrefix is the prefix we expect all columns to be preceded by, as in the syntax:
// <table name>.<column name>.
// if we don't expect this syntax, we must pass an empty string in the tablePrefix parameter.
func tableColumnNameToIndex(scheme tableScheme, tablePrefix string) map[string]uint32 {
	nameToIndex := map[string]uint32{}
	for i, column := range scheme.columns {
		columnName := strings.ToLower(column.columnName)
		if len(tablePrefix) > 0 {
			columnName = tablePrefix + "." + columnName
		}

		nameToIndex[columnName] = uint32(i)
	}
	return nameToIndex
}

func columnNamesToColumnIndexes(scheme tableScheme, nameToIndex map[string]uint32,
	columnNames []string) ([]uint32, error) {
	columnIndexes := make([]uint32, 0)
	for _, columnName := range columnNames {
		columnName = strings.ToLower(columnName)
		if index, exists := nameToIndex[columnName]; exists {
			columnIndexes = append(columnIndexes, index)
		} else {
			return nil, fmt.Errorf("no matching column %s in table", columnName)
		}
	}
	return columnIndexes, nil
}

func parseSingleOperand(operandString string, fieldType FieldType, index *uint32) (*operand, error) {
	if index == nil {
		parseFunc := FIELD_TYPE_QUERY_VALUE_PARSE[fieldType]
		data, err := parseFunc(operandString)
		if err != nil {
			return nil, err
		}

		return &operand{valueLiteral: data}, nil
	}

	return &operand{fieldIndex: index}, nil
}

func parseSingleConditionInternal(condStrings conditionStrings, columnsScheme []columnHeader,
	nameToIndex map[string]uint32) (*condition, error) {
	leftOperandIndex, leftOperandIsName := nameToIndex[condStrings.firstOperand]
	rightOperandIndex, rightOperandIsName := nameToIndex[condStrings.secondOperand]

	if !leftOperandIsName && !rightOperandIsName {
		return nil, fmt.Errorf("does not support comparison between two literals: %s, %s",
			condStrings.firstOperand, condStrings.secondOperand)
	}

	var valuesType FieldType
	if leftOperandIsName {
		valuesType = columnsScheme[leftOperandIndex].columnType
	} else {
		valuesType = columnsScheme[rightOperandIndex].columnType
	}

	var leftIndex *uint32 = nil
	if leftOperandIsName {
		leftIndex = &leftOperandIndex
	}
	leftOperand, err := parseSingleOperand(condStrings.firstOperand, valuesType, leftIndex)
	if err != nil {
		return nil, err
	}

	var rightIndex *uint32 = nil
	if rightOperandIsName {
		rightIndex = &rightOperandIndex
	}
	rightOperand, err := parseSingleOperand(condStrings.secondOperand, valuesType, rightIndex)
	if err != nil {
		return nil, err
	}

	condType := CONDITION_OPERATORS[condStrings.operator]

	if !isConditionSupported(valuesType, condType) {
		return nil, fmt.Errorf("condition type %d is not supported for values type %d", condType, valuesType)
	}

	return &condition{leftOperand: *leftOperand, rightOperand: *rightOperand,
		conditionType: CONDITION_OPERATORS[condStrings.operator]}, nil
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

func parseSingleCondition(sql string, columnsScheme []columnHeader, nameToIndex map[string]uint32,
	start int, end int) (*conditionNode, error) {
	for i := start; i < end; i++ {
		for operator := range CONDITION_OPERATORS {
			if sql[i:i+len(operator)] == operator {
				firstOperand := removeWhitespaces(sql[start:i])
				secondOperand := removeWhitespaces(sql[i+len(operator) : end])
				condStrings := conditionStrings{firstOperand: firstOperand,
					secondOperand: secondOperand, operator: operator}
				cond, err := parseSingleConditionInternal(condStrings, columnsScheme, nameToIndex)
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

func parseConditionByOperator(sql string, nameToIndex map[string]uint32, columnsScheme []columnHeader,
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
			node, err = parseConditionByOperator(sql, nameToIndex, columnsScheme, parenthesesIntervals,
				nextOperatorIndex, int(interval.start), int(interval.end))
		} else {
			node, err = parseCondition(sql, nameToIndex, columnsScheme, int(interval.start), int(interval.end))
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

func parseCondition(sql string, nameToIndex map[string]uint32, columnsScheme []columnHeader,
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
		node, err = parseCondition(sql, nameToIndex, columnsScheme,
			parenthesesIntervals[0].start, parenthesesIntervals[0].end)
	} else {
		if len(parenthesesIntervals) == 1 && isAtomicCondition(sql, parenthesesIntervals[0].start,
			parenthesesIntervals[0].end) {
			node, err = parseSingleCondition(sql, columnsScheme, nameToIndex,
				parenthesesIntervals[0].start, parenthesesIntervals[0].end)
		} else {
			node, err = parseConditionByOperator(sql, nameToIndex, columnsScheme, parenthesesIntervals, 0,
				start, end)
		}
	}

	if err != nil {
		return nil, err
	}
	return node, nil
}

// Finds the index of the end of the where statement in an SQL query
func endOfWhereStatement(sql string) int {
	// Go through all possible successors to the "where" statement

	// order by
	orderByIndex := strings.Index(sql, "order by")
	if orderByIndex != -1 {
		return orderByIndex
	}

	bestIndex := strings.Index(sql, "best")
	if bestIndex != -1 {
		return bestIndex
	}

	// no successor was found, return the end of the query
	return len(sql)
}

func parseWhereStatement(sql string, nameToIndex map[string]uint32,
	columnsScheme []columnHeader) (*conditionNode, error) {
	whereIndex := strings.Index(sql, "where")
	if whereIndex == -1 {
		return nil, nil
	}

	whereStatementEnd := endOfWhereStatement(sql)

	// TODO: handle queries with something after the condition
	whereStatement := sql[whereIndex+len("where") : whereStatementEnd]
	node, err := parseCondition(whereStatement, nameToIndex, columnsScheme, 0, len(whereStatement))
	if err != nil {
		return nil, err
	}
	return node, nil
}

func parseWhereStatementInQuery(db *openDB, sql string, tableID string) (*conditionNode, error) {
	tablePointer, err := findTable(db, tableID)
	if err != nil {
		return nil, err
	}
	tableHeaders := parseTableHeaders(db, *tablePointer)

	nameToIndex := tableColumnNameToIndex(tableHeaders.scheme, "")
	return parseWhereStatement(sql, nameToIndex, tableHeaders.scheme.columns)
}

func parseOrderByStatement(sql string, columnNameToIndex map[string]uint32) (*uint32, error) {
	orderByIndex := strings.Index(sql, "order by")
	if orderByIndex == -1 {
		// No order by is present in query
		return nil, nil
	}

	// TODO: handle other statements after "order by"
	orderByStatement := sql[orderByIndex+len("order by"):]
	orderByWords := stringToWords(orderByStatement)
	if len(orderByWords) == 0 {
		return nil, fmt.Errorf("invalid 'order by' statement %s", orderByStatement)
	}

	columnName := orderByWords[0]

	if index, exists := columnNameToIndex[columnName]; exists {
		return &index, nil
	} else {
		return nil, fmt.Errorf("invalid column name %s", columnName)
	}
}

func parseBestStatement(sql string) (*uint32, error) {
	bestIndex := strings.Index(sql, "best")
	if bestIndex == -1 {
		return nil, nil
	}

	bestStatement := sql[bestIndex+len("best"):]
	bestWords := stringToWords(bestStatement)
	if len(bestWords) != 1 {
		return nil, fmt.Errorf("invalid 'best' statement %s", bestStatement)
	}

	amountString := bestWords[0]
	amount, err := strconv.Atoi(amountString)
	if err != nil {
		return nil, fmt.Errorf("failed parsing best statement %s", bestStatement)
	}

	uintAmount := uint32(amount)

	return &uintAmount, nil
}

// Retrieves the type of the column names used by the SELECT query.
// There are two options:
// 1. Single table syntax - column names are represented by their name, without any prefix.
// 2. Multi table syntax - column names are prefixed by "<table name>."
// A mix of those two representation is not supported.
func selectQueryColumnNamesType(columnNames stringSet) (columnNameType, error) {
	columnNamesType := columnNameTypeUnknown

	for columnName := range columnNames {
		delimeterCount := strings.Count(columnName, ".")
		if delimeterCount > 1 {
			return columnNameTypeUnknown, fmt.Errorf("invalid column name %s", columnName)
		}

		if delimeterCount == 1 {
			if columnNamesType == columnNamesTypeSingleTable {
				return columnNameTypeUnknown, fmt.Errorf("found both single-table column name format as well as multiple tables format")
			}

			columnNamesType = columnNamesTypeMultipleTables
		} else {
			if columnNamesType == columnNamesTypeMultipleTables {
				return columnNameTypeUnknown, fmt.Errorf("found both single-table column name format as well as multiple tables format")
			}

			columnNamesType = columnNamesTypeSingleTable
		}
	}

	return columnNamesType, nil
}

// retrieves the column names relevant for the current query
func tableColumnsInQuery(tableColumns []columnHeader, prependTableName bool, tableName string,
	columnNamesInQuery stringSet) []string {
	columnsInQuery := make([]string, 0)
	for i := range tableColumns {
		columnName := tableColumns[i].columnName
		if prependTableName {
			columnName = tableName + "." + columnName
		}
		_, ok := columnNamesInQuery[strings.ToLower(columnName)]
		if !ok {
			// column was not requested
			continue
		}

		columnsInQuery = append(columnsInQuery, columnName)
	}

	return columnsInQuery
}

// retrieves a mpping of <column name>:<column offset>.
// all the columns offsets retrieved are local in the table, and must be fixed in order to get the joint
// table offset.
func selectQueryGetColumnNameToIndex(scheme tableScheme, tableID string, columnNamesType columnNameType,
	columnNames stringSet) (map[string]uint32, error) {
	tableNamePrefix := ""
	if columnNamesType == columnNamesTypeMultipleTables {
		tableNamePrefix = tableID
	}
	nameToIndex := tableColumnNameToIndex(scheme, tableNamePrefix)
	return nameToIndex, nil
}

// retrieves the columnName-to-index map of the joint table (where the index of each column is
// its index in the joint table) and all the column headers (in the order in which they appear
// in the joint table).
func selectQueryGetTablesScheme(db *openDB, columnNames []string, tableIDs []string) (
	map[string]uint32, []columnHeader, error) {
	nameToIndex := make(map[string]uint32)
	columnsScheme := make([]columnHeader, 0)

	columnNamesSet := stringListToSet(columnNames)

	columnNamesType, err := selectQueryColumnNamesType(columnNamesSet)
	if err != nil {
		return nil, nil, err
	}

	// this variable used to "advance" the column offsets according to the sum of
	// the previous joint tables.
	// this is required sinece we treat the final record as one big record containing
	// all columns in all the joint tables.
	columnsOffset := 0
	for _, tableID := range tableIDs {
		tablePointer, err := findTable(db, tableID)
		if err != nil {
			return nil, nil, err
		}
		tableHeaders := parseTableHeaders(db, *tablePointer)

		newNameToIndex, err :=
			selectQueryGetColumnNameToIndex(tableHeaders.scheme, tableID, columnNamesType, columnNamesSet)
		if err != nil {
			return nil, nil, err
		}

		for columnName := range newNameToIndex {
			nameToIndex[columnName] = newNameToIndex[columnName] + uint32(columnsOffset)
		}

		columnsScheme = append(columnsScheme, tableHeaders.scheme.columns...)

		columnsOffset += len(tableHeaders.scheme.columns)
	}

	return nameToIndex, columnsScheme, nil
}

func selectQueryResolveColumnOffsets(columnNames []string, nameToIndex map[string]uint32) ([]uint32, error) {
	selectColumns := make([]uint32, 0)
	for _, name := range columnNames {
		offset, ok := nameToIndex[name]
		if !ok {
			return nil, &nonExistentColumnError{
				Description: fmt.Sprintf("non-existent column name %s was supplied", name)}
		}

		selectColumns = append(selectColumns, offset)
	}

	return selectColumns, nil
}

func parseSelectQuery(db *openDB, sql string) (*selectQuery, error) {
	words := stringToWords(sql)
	tableIDs := tableIDsFromQuery(words, "from")
	if tableIDs == nil {
		return nil, fmt.Errorf("invalid 'from' clause in sql statement %s", sql)
	}

	columnNames, err := columnNamesFromQuery(words)
	if err != nil {
		return nil, err
	}

	nameToIndex, columnsScheme, err := selectQueryGetTablesScheme(db, columnNames, tableIDs)
	if err != nil {
		return nil, err
	}

	cond, err := parseWhereStatement(sql, nameToIndex, columnsScheme)
	if err != nil {
		return nil, err
	}

	orderBy, err := parseOrderByStatement(sql, nameToIndex)
	if err != nil {
		return nil, err
	}

	bestAmount, err := parseBestStatement(sql)
	if err != nil {
		return nil, err
	}

	selectColumns, err := selectQueryResolveColumnOffsets(columnNames, nameToIndex)
	if err != nil {
		return nil, err
	}

	return &selectQuery{columnNames: columnNames, columns: selectColumns, tableIDs: tableIDs,
		condition: cond, orderBy: orderBy, bestAmount: bestAmount}, nil
}

func parseSingleValuesTuple(statement string, index *int) ([]string, error) {
	if statement[*index] != '(' {
		return nil, fmt.Errorf("values tuple should start with '('")
	}
	*index++
	start := *index
	for ; *index < len(statement); *index++ {
		if statement[*index] == ')' {
			break
		}
	}
	if statement[*index] != ')' {
		return nil, fmt.Errorf("failed to find ')' at the end of values tuple")
	}
	valuesString := statement[start:*index]
	*index++
	return strings.Split(valuesString, ","), nil
}

func valuesTupleIntoRecord(values []string, scheme tableScheme) (*Record, error) {
	if len(values) != len(scheme.columns) {
		return nil, fmt.Errorf("expected %d values, instead got %d in %s", len(scheme.columns),
			len(values), strings.Join(values, ","))
	}
	fields := make([]Field, 0)
	for i := range values {
		trimmedVal := strings.Trim(values[i], " \t\r\n")
		field, err := STRING_TO_FIELD_FUNCS[scheme.columns[i].columnType](trimmedVal)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return &Record{Fields: fields}, nil
}

func findValuesStringInInsertQuery(sql string) (*string, error) {
	r, err := regexp.Compile("values\\s+(\\(.*\\))")
	if err != nil {
		return nil, err
	}

	// TODO: make this prettier
	matches := r.FindStringSubmatch(sql)
	if len(matches) != 2 {
		return nil, fmt.Errorf("unexpected match from sql %s", sql)
	}
	return &matches[1], nil
}

func parseInsertQueryValues(sql string, scheme tableScheme) ([]Record, error) {
	records := make([]Record, 0)

	valuesString, err := findValuesStringInInsertQuery(sql)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(*valuesString); i++ {
		// TODO: make sure we encounter exactly one comma, no more and no less
		// between parantheses
		if isWhitespace(rune((*valuesString)[i])) || (*valuesString)[i] == ',' {
			continue
		}
		tupleStrings, err := parseSingleValuesTuple(*valuesString, &i)
		if err != nil {
			return nil, err
		}
		record, err := valuesTupleIntoRecord(tupleStrings, scheme)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	return records, nil
}

func parseInsertQuery(db *openDB, sql string) (*insertQuery, error) {
	words := stringToWords(sql)
	tableID, err := tableIDFromQuery(words, "into")
	if err != nil {
		return nil, err
	}

	tablePointer, err := findTable(db, tableID)
	if err != nil {
		return nil, err
	}
	tableHeaders := parseTableHeaders(db, *tablePointer)
	records, err := parseInsertQueryValues(sql, tableHeaders.scheme)
	if err != nil {
		return nil, err
	}

	return &insertQuery{tableID: tableID, records: records}, nil
}

// TODO: remove code duplication between this functon and parseInsertQuery
func parseDeleteQuery(db *openDB, sql string) (*deleteQuery, error) {
	words := stringToWords(sql)
	tableID, err := tableIDFromQuery(words, "from")
	if err != nil {
		return nil, err
	}

	cond, err := parseWhereStatementInQuery(db, sql, tableID)
	if err != nil {
		return nil, err
	}

	return &deleteQuery{tableID: tableID, condition: cond}, nil
}

// Retrieve the single interval which belongs to a statement surrounded by parentheses
// if none is found or there is more than one, return nil
func singleParanthesesInterval(intervals []parenthesesInterval) *parenthesesInterval {
	var result *parenthesesInterval
	for _, interval := range intervals {
		if interval.parentheses && result != nil {
			return nil
		}

		if interval.parentheses && result == nil {
			result = &interval
		}
	}

	return result
}

func parseCreateQuery(db *openDB, sql string) (*createQuery, error) {
	words := stringToWords(sql)
	tableID, err := tableIDFromQuery(words, "table")
	if err != nil {
		return nil, err
	}

	intervals, err := divideStatementByParentheses(sql, 0, len(sql))
	if err != nil {
		return nil, err
	}

	headersStatementInterval := singleParanthesesInterval(intervals)
	if headersStatementInterval == nil {
		return nil, fmt.Errorf("faulty create query %s", sql)
	}

	headersStatemt := sql[headersStatementInterval.start:headersStatementInterval.end]
	headerStrings := strings.Split(headersStatemt, ",")
	headers := make([]columnHeader, 0)
	for _, headerString := range headerStrings {
		headerWords := stringToWords(headerString)
		if len(headerWords) != 2 {
			return nil, fmt.Errorf("%s is not a valid column header", headerString)
		}
		columnName := headerWords[0]
		columnTypeStr := headerWords[1]
		columnType, ok := FIELD_STRING_TO_TYPE[columnTypeStr]
		if !ok {
			return nil, fmt.Errorf("no such column type %s", columnTypeStr)
		}
		headers = append(headers, columnHeader{columnName: columnName, columnType: columnType})
	}
	return &createQuery{tableID: tableID, scheme: tableScheme{columns: headers}}, nil
}

func getUpdateSetStatement(sql string) (string, error) {
	setIndex := strings.Index(sql, "set")
	if setIndex == -1 {
		return "", fmt.Errorf("no set statement found in update query")
	}
	whereIndex := strings.Index(sql, "where")
	if whereIndex == -1 {
		// No where statement in query, this is a valid case
		whereIndex = len(sql)
	}

	if whereIndex < setIndex {
		return "", fmt.Errorf(
			"where statement cannot appear before set statement in update query")
	}
	return removeWhitespaces(sql[setIndex+len("set") : whereIndex]), nil
}

func parseUpdateSetStatement(db *openDB, sql string, scheme tableScheme) (*recordUpdate, error) {
	setStatement, err := getUpdateSetStatement(sql)
	if err != nil {
		return nil, err
	}

	nameToIndex := tableColumnNameToIndex(scheme, "")
	setAssignments := strings.Split(setStatement, ",")
	changes := make([]recordChange, 0)
	for _, assignment := range setAssignments {
		operands := strings.Split(assignment, "=")
		if len(operands) != 2 {
			return nil, fmt.Errorf("invalid set assignment %s", assignment)
		}
		columnName, val := operands[0], operands[1]

		fieldIndex, exists := nameToIndex[columnName]
		if !exists {
			return nil, fmt.Errorf("no such column name %s (in set statement %s)", columnName, assignment)
		}
		byteVal, err := FIELD_TYPE_QUERY_VALUE_PARSE[scheme.columns[fieldIndex].columnType](val)
		if err != nil {
			return nil, fmt.Errorf("invalid value for field %s : %s", columnName, val)
		}
		changes = append(changes, recordChange{fieldIndex: int(fieldIndex), newData: byteVal})
	}

	return &recordUpdate{changes: changes}, nil
}

func parseUpdateQuery(db *openDB, sql string) (*updateQuery, error) {
	words := stringToWords(sql)
	tableID, err := tableIDFromQuery(words, "update")
	if err != nil {
		return nil, err
	}

	cond, err := parseWhereStatementInQuery(db, sql, tableID)
	if err != nil {
		return nil, err
	}

	headers, err := getTableHeaders(db, tableID)
	if err != nil {
		return nil, err
	}

	update, err := parseUpdateSetStatement(db, sql, headers.scheme)
	if err != nil {
		return nil, err
	}

	return &updateQuery{tableID: tableID, condition: cond, update: *update}, nil
}

func parseQueryType(sql string) (queryType, error) {
	words := stringToWords(sql)
	if len(words) == 0 {
		return QueryTypeInvalid, fmt.Errorf("empty query")
	}
	return QUERY_TYPE_MAP[words[0]], nil
}

// Perform normalizations required for any SQL query
func normalizeQuery(sql string) string {
	sql = strings.ToLower(sql)

	// Newlines are practically identical to spaces in SQL, and allow us to simplify the query
	sql = strings.Replace(sql, "\n", " ", -1)
	return sql
}
