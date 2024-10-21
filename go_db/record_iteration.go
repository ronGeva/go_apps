/*
This module contains logic related to iteration of records in a table.
*/

package go_db

import "errors"

// this struct contains all the information needed to modify a record from a table (update/deletion)
// - its index (used to locate and delete/modify it)
// - a partial record containing only  its key fields (used to remove it from the relevant
// indexes/modify its keys in the index)
type recordForChange struct {
	index         uint32
	partialRecord Record
}

type jointRecord struct {
	record  Record
	offsets []uint32
}

type jointTableRecordIterator struct {
	tableIterators []innerTableRecordIterator
	currentRecords []*tableCurrentRecord
}

func initializeJointTableRecordIterator(db *openDB, tableIDs []string) (*jointTableRecordIterator, error) {
	tableIterators := make([]innerTableRecordIterator, 0)
	currentRecords := make([]*tableCurrentRecord, 0)
	for _, tableID := range tableIDs {
		iterator, err := initializeInternalRecordIterator(db, tableID)
		if err != nil {
			return nil, err
		}
		tableIterators = append(tableIterators, *iterator)
		// enter an empty "current record"
		currentRecords = append(currentRecords, nil)
	}

	return &jointTableRecordIterator{tableIterators: tableIterators, currentRecords: currentRecords}, nil
}

func (iterator *jointTableRecordIterator) resetIterators(offset int) {
	if offset == len(iterator.tableIterators) {
		return
	}

	iterator.resetIterators(offset + 1)
	iterator.tableIterators[offset].reset()
}

func (iterator *jointTableRecordIterator) currentRecord(offset int) *tableCurrentRecord {
	if iterator.currentRecords[offset] == nil {
		// the current iterator was yet to be initialized
		record := iterator.tableIterators[offset].next()
		if record == nil {
			return nil
		}

		iterator.currentRecords[offset] = record
	}

	return iterator.currentRecords[offset]
}

func (iterator *jointTableRecordIterator) advanceRecord(offset int) {
	iterator.currentRecords[offset] = iterator.tableIterators[offset].next()
}

// joins a new record (which is appended "to the left") with a joint record
func joinRecords(left *tableCurrentRecord, right *jointRecord) *jointRecord {
	record := Record{Fields: append(left.record.Fields, right.record.Fields...),
		Provenance: append(left.record.Provenance, right.record.Provenance...)}
	jointOffsets := append([]uint32{left.offset}, right.offsets...)
	return &jointRecord{record: record, offsets: jointOffsets}
}

func (iterator *jointTableRecordIterator) nextInner(offset int) *jointRecord {
	if offset == len(iterator.tableIterators)-1 {
		iterator.advanceRecord(offset)
		record := iterator.currentRecord(offset)
		if record == nil {
			return nil
		}

		return &jointRecord{record: record.record, offsets: []uint32{record.offset}}
	}

	currentRecord := iterator.currentRecord(offset)
	if currentRecord == nil {
		// finished iterating the current table, propogate it up so that the upper level of recursion
		// will advance their "current record"
		return nil
	}

	restOfTablesJointRecord := iterator.nextInner(offset + 1)
	if restOfTablesJointRecord != nil {
		// return the current record with the next variation of the rest of the tables' joint record
		return joinRecords(currentRecord, restOfTablesJointRecord)
	}

	// we've finished iterating the rest of the tables, but we still have more entries to go through in
	// the current table.
	// get the next entry in this table,  reset the iterators of all next tables and continue
	// from there.
	iterator.advanceRecord(offset)
	iterator.resetIterators(offset + 1)

	// recursively call ourselves
	// since we've advanced the current iterator and reset all next tables,
	// we will either succeed in generating the next iteration, or finish
	return iterator.nextInner(offset)
}

func (iterator *jointTableRecordIterator) next() *jointRecord {
	record := iterator.nextInner(0)
	if record != nil {
		provenanceApplyJoin(record)
	}
	return record
}

type mapFunctionType[outputType any, mapInput any] func(context recordContext, input mapInput) outputType

func filterRecordsWorker[outputType any, mapInput any](recordsChannel <-chan recordContext,
	mapFunction mapFunctionType[outputType, mapInput], cond *conditionNode, outChannel chan<- outputType,
	input mapInput) {
	for {
		context, ok := <-recordsChannel
		if !ok {
			// Done
			return
		}

		if cond == nil || checkAllConditions(*cond, context.record) {
			output := mapFunction(context, input)
			outChannel <- output
		}
	}
}

func getPartialRecord(record *Record, offsets partialRecordOffsets) Record {
	partialRecord := Record{Fields: make([]Field, 0), Provenance: make([]ProvenanceField, 0)}

	if offsets.columns == nil {
		partialRecord.Fields = record.Fields
	}
	for _, index := range offsets.columns {
		partialRecord.Fields = append(partialRecord.Fields, record.Fields[index])
	}

	if offsets.provenances == nil {
		partialRecord.Provenance = record.Provenance
	}
	for _, index := range offsets.provenances {
		partialRecord.Provenance = append(partialRecord.Provenance, record.Provenance[index])
	}

	return partialRecord
}

func getPartialRecords(records []Record, offsets partialRecordOffsets) []Record {
	partialRecords := make([]Record, len(records))
	for i := 0; i < len(records); i++ {
		partialRecords[i] = getPartialRecord(&records[i], offsets)
	}
	return partialRecords
}

func mapGetRecords(context recordContext, requestedColumns []uint32) Record {
	record := context.record
	if requestedColumns == nil {
		return record
	}

	return getPartialRecord(&record, partialRecordOffsets{columns: requestedColumns})
}

func mapGetRecordForChange(context recordContext, offsets partialRecordOffsets) recordForChange {
	partialRecord := getPartialRecord(&context.record, offsets)
	return recordForChange{partialRecord: partialRecord, index: context.index}
}

func waitForWorkers[outputType any](outChannel <-chan outputType, doneChannel <-chan bool, numOfWorkers uint32) []outputType {
	output := make([]outputType, 0)
	workersDone := uint32(0)
	// Read output from workers while keeping track of the number of finished workers
	for workersDone < numOfWorkers {
		select {
		case out := <-outChannel:
			output = append(output, out)
		case <-doneChannel:
			workersDone++
		}
	}

	// All workers are done.
	// Read the messages that wait in the channel
	for {
		select {
		case out := <-outChannel:
			output = append(output, out)
		default:
			// No more messages
			return output
		}
	}
}

func mapEachRecord[outputType any, mapInputType any](openDatabase *openDB, tableIDs []string,
	recordsCondition *conditionNode, mapFunction mapFunctionType[outputType, mapInputType],
	mapInput mapInputType) ([]outputType,
	error) {
	recordsChannel := make(chan recordContext, 1000)
	outChannel := make(chan outputType, 1000)
	const numOfWorkers = 5
	doneChannel := make(chan bool, numOfWorkers)

	for i := 0; i < 5; i++ {
		go func() {
			defer func() {
				doneChannel <- true
			}()
			filterRecordsWorker(recordsChannel, mapFunction, recordsCondition, outChannel, mapInput)
		}()
	}

	ok, err := validateConditionsJointTable(openDatabase, tableIDs, recordsCondition)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("conditions don't match joint table scheme")
	}

	recordsIterator, err := initializeJointTableRecordIterator(openDatabase, tableIDs)
	if err != nil {
		return nil, err
	}

	record := recordsIterator.next()
	for record != nil {
		// TODO: support supplying the records channel with more than just the first offset
		recordsChannel <- recordContext{record: record.record, index: record.offsets[0]}
		record = recordsIterator.next()
	}

	close(recordsChannel)
	output := waitForWorkers(outChannel, doneChannel, numOfWorkers)

	return output, nil
}
