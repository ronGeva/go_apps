/*
This module contains logic related to iteration of records in a table.
*/

package go_db

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

func mapGetRecords(context recordContext, requestedColumns []uint32) Record {
	record := context.record
	if requestedColumns == nil {
		return record
	}

	newRecord := Record{}
	for _, index := range requestedColumns {
		newRecord.Fields = append(newRecord.Fields, record.Fields[index])
	}

	return newRecord
}

func mapGetRecordIndexes(context recordContext, unused interface{}) uint32 {
	return context.index
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

func mapEachRecord[outputType any, mapInputType any](openDatabase *openDB, tableID string,
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

	err := writeAllRecordsToChannel(openDatabase, tableID, recordsCondition, recordsChannel)
	if err != nil {
		return nil, err
	}

	close(recordsChannel)
	output := waitForWorkers(outChannel, doneChannel, numOfWorkers)

	return output, nil
}
