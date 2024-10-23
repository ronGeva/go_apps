package go_db

// This module contains logic that allows users to retrieve the top K records in a given joint table via their provenance.
// Each provenance field has a pre-determined ranking which signifies how reliable the data is.
// A joint record has a provenance composed out of all the atomic records from which it was created, with an operator "*"
// between them.
// For example, given 3 records A, B, C, each with 2 provenance fields [[A_1, A_2], [B_1, B_2], [C_1, C_2]], the joint
// record will have two provenance fields: [A_1 * B_1 * C_1, A_2 * B_2 * C_2].
//
// In order to calculate the overall provenance of a record we must somehow aggregate all those values into a single
// provenance score.
// This is done via an monotonous non-decreasing aggregation function.
// The provenance operator "*" and "+" each have such function, and we have such a function for aggregating
// the combined score of multiple provenance fields of different types.
// The algorithm presented here assumes we receive the aggregation function of multi-provenance fields record
// as an input.
//
// The usage of this module is to efficiently retrieve the top most reliable records out of the entire joint
// table.

import (
	"fmt"
	"sort"

	"github.com/ronGeva/go_apps/b_tree"
)

// This struct describes a single joint record retrieved by the provenance table iterator
// it contains the actual joint record, which contains the joint data and the joint tables' record offsets
// from which this joint record was created.
//
// It also holds the index offsets, which are the indexes of the records used to compose the joint record in the
// indexes of each table.
type provIndexRecord struct {
	record       jointRecord
	indexOffsets []uint32
}

// An iterator which retrieves joint records out of a joint table in order of their provenance, given a specific
// provenance type that we're interested in.
type provenanceTableIterator struct {
	// iterators to the underlying provenance column indexes of each of the tables
	iterators []indexTableIterator

	// the next candidates to be the top record
	// use a BTree to allow for quick insert/deletion and to easily retrieve the current smallest value
	candidates b_tree.BTree

	// since we can't save records directly in the candidates BTree, we save pointers for them.
	// in order to use unique pointers, we keep an increasing counter for each new candidate we
	// add.
	pointerCounter uint32

	// a mapping between the pointer in a candidates pair and the list of tuples which have a score
	// matching the pair's key.
	candidatesMapping map[b_tree.BTreePointer][]provIndexRecord

	// records we've retrieved for each of the tables
	// at index [i][j] we'll have the j-th record of the i-th table
	// This member is needed since we want to retrieve the same record for each joint record in which it
	// is a part of.
	retrievedRecords [][]tableCurrentRecord

	// a set containing all visited tuples
	// the key of this map is the stringified version of the tuple (which uniquely differentiates it
	// from all other possible tuples).
	visited map[string]interface{}

	// the provenance type we're interested in
	provType ProvenanceType
}

// pop the next best candidate from the candidates BTree.
func (iterator *provenanceTableIterator) popBestCandidate() *provIndexRecord {
	candidatesIterator := iterator.candidates.Iterator()
	if candidatesIterator == nil {
		// finished iterating all entries
		return nil
	}

	// get next candidate with smallest score
	pair := candidatesIterator.Next()
	if pair == nil {
		return nil
	}

	// arbitrarily choose the first element from all candidates with equal score
	candidates, ok := iterator.candidatesMapping[pair.Pointer]
	assert(ok, "failed get mapping of candidate pointer")
	assert(len(candidates) > 0, "got an empty same-score-candidates list")
	bestCandidate := candidates[0]

	// remove the candidate chosen from the list of tuples with identical scores.
	// delete the key entirely if the list is now empty
	if len(candidates) == 1 {
		delete(iterator.candidatesMapping, pair.Pointer)
		iterator.candidates.Delete(pair.Key)
	} else {
		iterator.candidatesMapping[pair.Pointer] = candidates[1:]
	}

	return &bestCandidate
}

// given a list of the indexes of each record in a joint record, generate the record itself.
// we assume all records at those offsets were already cached in the "retrievedRecords" member.
func (iterator *provenanceTableIterator) generateRecord(offsets []uint32) *provIndexRecord {
	record := jointRecord{}
	// go in reverse order in order to create the record's field in the correct order
	for tableOffset := len(offsets) - 1; tableOffset >= 0; tableOffset-- {
		recordOffset := offsets[tableOffset]
		assert(tableOffset < len(iterator.retrievedRecords),
			"got a candidate tuple bigger than expected table amount")

		assert(int(recordOffset) < len(iterator.retrievedRecords[tableOffset]),
			"got record offset of a future record which we shouldn't have tried accessing so soon")

		currentRecord := iterator.retrievedRecords[tableOffset][recordOffset]

		record = *joinRecords(&currentRecord, &record)
	}

	provenanceApplyJoin(&record)
	return &provIndexRecord{indexOffsets: offsets, record: record}
}

// get the provenance score of the record (we only care for the provenance type this iterator
// was initialized for).
func (iterator *provenanceTableIterator) recordProvenance(record *Record) ProvenanceScore {
	for _, provField := range record.Provenance {
		if provField.Type == iterator.provType {
			return provField.Score()
		}
	}

	assert(false, "failed to find expected provenance field in provenanceTableIterator")
	// return value is required for the code to compile, we will never reach this flow
	return 0
}

// add a new candidate for the "top record" into the candidates BTree.
// we assume all the records required to generate this joint record were already cached beforehand
// in the "retrievedRecords" member.
func (iterator *provenanceTableIterator) addNewCandidate(candidate []uint32) {
	candidateStringKey := fmt.Sprint(candidate)

	_, returned := iterator.visited[candidateStringKey]
	if returned {
		return // we've already added this candidate before
	}

	candidateRecord := iterator.generateRecord(candidate)
	candidateScore := iterator.recordProvenance(&candidateRecord.record.record)
	existingPointer := iterator.candidates.Get(b_tree.BTreeKeyType(candidateScore))
	var pointer b_tree.BTreePointer
	if existingPointer == nil {
		pointer = b_tree.BTreePointer(iterator.pointerCounter)
		iterator.pointerCounter++

		iterator.candidatesMapping[pointer] = make([]provIndexRecord, 0)
		// no candidate with such score was added to the candidate tree, add it now
		iterator.candidates.Insert(b_tree.BTreeKeyPointerPair{
			Key: b_tree.BTreeKeyType(candidateScore), Pointer: pointer})
	} else {
		pointer = *existingPointer
	}

	iterator.candidatesMapping[pointer] = append(iterator.candidatesMapping[pointer], *candidateRecord)

	// mark as "visited to avoid adding it to candidates tree again"
	iterator.visited[candidateStringKey] = nil
}

// cache the atomic record at tables[tableOffset][recordOffset].
// returns whether we've succeeded in caching the required record
func (iterator *provenanceTableIterator) cacheNextTableRecord(tableOffset uint32, recordOffset uint32) bool {
	assert(int(recordOffset) <= len(iterator.retrievedRecords[tableOffset]), "invalid record offset")
	if int(recordOffset) < len(iterator.retrievedRecords[tableOffset]) {
		// we've already retrieved this record, no more work is required
		return true
	}

	// retrieve the next record in the table, according to its index iterator
	record := iterator.iterators[tableOffset].next()
	if record == nil {
		// we've finished iterating this table, we will get no more candidates by increasing the offset
		// of this table
		return false
	}

	iterator.retrievedRecords[tableOffset] =
		append(iterator.retrievedRecords[tableOffset], *record)

	return true
}

// add all the new candidates given the current best record's offset.
// we do so by generating len(tables) new candidates by increasing by one each of the
// "currentBest" record's indexes by 1.
// we cache the required records for each new candidate then add it to the candidates
// tree (if it wasn't added already in the past).
func (iterator *provenanceTableIterator) addNewCandidates(currentBest []uint32) {
	for tableOffset := 0; tableOffset < len(currentBest); tableOffset++ {
		recordOffset := currentBest[tableOffset]
		recordOffset++

		if !iterator.cacheNextTableRecord(uint32(tableOffset), recordOffset) {
			continue
		}

		candidate := make([]uint32, len(currentBest))
		copy(candidate, currentBest)
		candidate[tableOffset]++

		iterator.addNewCandidate(candidate)
	}
}

// get the next best record (according to the chosen provenance type)
func (iterator *provenanceTableIterator) next() *jointRecord {
	// pop the best candidate we've pre-calculated
	record := iterator.popBestCandidate()

	if record == nil {
		return nil
	}

	// add new candidates into the relevant data structures
	iterator.addNewCandidates(record.indexOffsets)

	return &record.record
}

// adds the first "best candidate" into the candidates tree (that would be the candidates whose
// offsets are (0, 0, ... 0))
func (iterator *provenanceTableIterator) initialize() error {

	firstCandidate := make([]uint32, len(iterator.iterators))
	for i := 0; i < len(iterator.iterators); i++ {
		record := iterator.iterators[i].next()
		if record == nil {
			return fmt.Errorf("table %d is empty, cannot initialize provenance iterator", i)
		}
		iterator.retrievedRecords[i] = []tableCurrentRecord{*record}

		firstCandidate[i] = 0
	}
	iterator.addNewCandidate(firstCandidate)
	return nil
}

// initializes and returns a provenanceTableIterator through which we can iterate a joint table's records best to worst,
// according to a specific provenance type.
func provenanceInitializeTableIterator(db *openDB, tableIds []string, provType ProvenanceType) (*provenanceTableIterator, error) {
	provOffset := -1
	for i := 0; i < len(db.provFields); i++ {
		if provType == db.provFields[i].Type {
			provOffset = i
		}
	}
	if provOffset == -1 {
		return nil, fmt.Errorf("invalid provType %d passed", int(provType))
	}

	iterators := make([]indexTableIterator, 0)

	for _, tableId := range tableIds {
		iterator, err := indexInitializeTableIterator(db, tableId, uint32(provOffset), true)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize provenance index iterator %d for table %s",
				provOffset, tableId)
		}
		iterators = append(iterators, *iterator)
	}

	candidates, err := b_tree.InitializeBTree(b_tree.InitializeInMemoryPersistency())
	if err != nil {
		return nil, err
	}

	provIterator := provenanceTableIterator{iterators: iterators, candidates: *candidates,
		pointerCounter: 0, provType: provType,
		retrievedRecords:  make([][]tableCurrentRecord, len(tableIds)),
		candidatesMapping: make(map[b_tree.BTreePointer][]provIndexRecord),
		visited:           make(map[string]interface{})}

	err = provIterator.initialize()
	if err != nil {
		return nil, err
	}

	return &provIterator, nil
}

// Iterate over a joint table's record according to the aggregated score of
// all the record's provenance fields.
// This is done via Fagin's Rank Aggregation Algorithm.
type provenanceAggregatedTableIterator struct {
	// Maps a joint record unique ID with the amount of times it has been seen on
	// different provenance lists.
	// Once a record has been seen on all lists, we can assume it has a bigger aggregated
	// score than all non-seen records and we can return it.
	recordSeenCounter map[string]uint32

	// a map containing the records we've already retrieved.
	// the key to this map is the stringified tuple of the joint records' offsets (as in
	// the offsets in the table of each record from which this joint record is created).
	//
	// the record offsets used here are different than the ones used in provenanceTableIterator,
	// as their we use the offsets of the records within the provenance indexes of each record.
	retrievedRecords map[string]jointRecord

	// Iterators used to retrieve the next best joint record according to each provenance
	// field.
	iterators []provenanceTableIterator

	// The function used to aggregate the provenance score of a record according to each specific
	// provenance field into a single score.
	aggregation ProvenanceAggregationFunc

	// a list containing the records we've already seen on all different provenance-ordered lists
	// (each generated by iterating a different provenanceTableIterator).
	seenOnAllLists []jointRecord

	// the conditions we want each record to uphold in order for us to retrieve it
	conditions *conditionNode
}

// performs a single "loop" over all different provenance-ordered lists.
// retrieve a record from each provenance iterator then adds it to the "retrievedRecords" and
// the "seenOnAllLists" members accordingly.
// returns true if finished
func (iterator *provenanceAggregatedTableIterator) advance() bool {
	for i := 0; i < len(iterator.iterators); i++ {
		provIterator := &iterator.iterators[i]
		record := provIterator.next()
		if record == nil {
			// All iterators should retrieve the same amount of records (since they're
			// all pointing to the same joint table).
			// Therefore, if a single iterator returns nil, all should do the same from
			// now on.
			return true
		}

		// check if the record matches the condition
		if iterator.conditions != nil && !checkAllConditions(*iterator.conditions, record.record) {
			continue
		}

		recordStringKey := fmt.Sprint(record.offsets)
		val, ok := iterator.recordSeenCounter[recordStringKey]
		if !ok {
			val = 0
			iterator.retrievedRecords[recordStringKey] = *record
		}

		val++
		iterator.recordSeenCounter[recordStringKey] = val

		if int(val) == len(iterator.iterators) {
			iterator.seenOnAllLists = append(iterator.seenOnAllLists, *record)
		}
	}

	return false
}

func provenanceInitializeAggregatedTableIterator(db *openDB, tableIds []string,
	aggregation ProvenanceAggregationFunc, conditions *conditionNode) (
	*provenanceAggregatedTableIterator, error) {
	provIterators := make([]provenanceTableIterator, 0)
	for _, provField := range db.provFields {
		provIterator, err := provenanceInitializeTableIterator(db, tableIds, provField.Type)
		if err != nil {
			return nil, err
		}
		provIterators = append(provIterators, *provIterator)
	}

	return &provenanceAggregatedTableIterator{
			iterators:         provIterators,
			recordSeenCounter: make(map[string]uint32),
			retrievedRecords:  make(map[string]jointRecord),
			aggregation:       aggregation,
			seenOnAllLists:    make([]jointRecord, 0),
			conditions:        conditions},
		nil
}

type provRankedRecord struct {
	record Record
	score  ProvenanceScore
}

type provRankedRecords struct {
	records []provRankedRecord
}

func (records *provRankedRecords) Len() int {
	return len(records.records)
}

func (records *provRankedRecords) Less(i, j int) bool {
	leftKey := records.records[i].score
	rightKey := records.records[j].score
	return leftKey < rightKey
}

func (records *provRankedRecords) Swap(i, j int) {
	temp := records.records[i]
	records.records[i] = records.records[j]
	records.records[j] = temp
}

// retrieves the top <amount> best joint records according to their aggregated provenance score.
// the records retrieved will be ordered according to their aggregated provenance score.
func provenanceGetTopRecords(db *openDB, tables []string, aggregation ProvenanceAggregationFunc,
	amount uint32, conditions *conditionNode) (
	[]Record, error) {
	iterator, err := provenanceInitializeAggregatedTableIterator(db, tables, aggregation, conditions)
	if err != nil {
		return nil, err
	}

	// continue iterating the aggregated provenance iterator until we've seen the amount of records
	// we're looking for on all lists.
	// once that is the case - we can be sure all records we've yet to see have a worse aggregated
	// score than the ones we've seen on all lists.
	// we can then sort the records we've seen and return the best of them.
	for len(iterator.seenOnAllLists) < int(amount) {
		if iterator.advance() {
			// no more entries
			break
		}
	}

	records := make([]Record, 0)
	for key := range iterator.retrievedRecords {
		record := iterator.retrievedRecords[key]
		records = append(records, record.record)
	}

	rankedRecords := provRankedRecords{records: make([]provRankedRecord, 0)}
	for _, record := range records {
		provScores := make([]ProvenanceScore, 0)
		for _, provField := range record.Provenance {
			provScores = append(provScores, provField.Score())
		}
		aggregatedScore := aggregation(provScores)
		rankedRecords.records = append(rankedRecords.records, provRankedRecord{record: record, score: aggregatedScore})
	}

	sort.Sort(&rankedRecords)

	finalAmount := min(int(amount), len(rankedRecords.records))

	bestRecords := make([]Record, finalAmount)
	for i := 0; i < finalAmount; i++ {
		bestRecords[i] = rankedRecords.records[i].record
	}

	return bestRecords, nil
}
