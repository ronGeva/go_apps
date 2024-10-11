package go_db

import (
	"encoding/binary"
	"fmt"

	"github.com/ronGeva/go_apps/b_tree"
)

type ProvenanceType uint16
type ProvenanceScore uint32
type ProvenanceOperator uint8

const (
	ProvenanceTypeConnection ProvenanceType = iota
	ProvenanceTypeAuthentication
)

const (
	ProvenanceOperatorNil = iota
	ProvenanceOperatorPlus
	ProvenanceOperatorMultiply
)

func provenanceConnectionStringify(field Field) string {
	blobField, ok := field.(BlobField)
	if !ok {
		return "<faulty connection provenance>"
	}

	return blobField.Stringify()
}

func deserializeProvenanceAuthenticationField(data []byte) ProvenanceAuthentication {
	i := 0
	userSize := binary.LittleEndian.Uint16(data[i : i+2])
	i += 2

	username := string(data[i : i+int(userSize)])
	i += int(userSize)

	passSize := binary.LittleEndian.Uint16(data[i : i+2])
	i += 2

	pass := string(data[i : i+int(passSize)])
	return ProvenanceAuthentication{user: username, password: pass}
}

func serializeProvenanceConnectionField(connection ProvenanceConnection) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data[:4], connection.ipv4)

	return data
}

func serializeProvenanceAuthenticationField(auth ProvenanceAuthentication) []byte {
	userLen := len([]byte(auth.user))
	passLen := len([]byte(auth.password))
	totalSize := 2 + userLen + 2 + passLen
	data := make([]byte, totalSize)

	i := 0

	binary.LittleEndian.PutUint16(data[i:i+2], uint16(userLen))
	i += 2

	copy(data[i:i+userLen], []byte(auth.user))
	i += userLen

	binary.LittleEndian.PutUint16(data[i:i+2], uint16(passLen))
	i += 2

	copy(data[i:i+passLen], []byte(auth.password))
	i += passLen

	return data
}

func provenanceAuthenticationStringify(field Field) string {
	blobField, ok := field.(BlobField)
	if !ok {
		return "<faulty authentication provenance>"
	}

	authentication := deserializeProvenanceAuthenticationField(blobField.Data)
	return fmt.Sprintf("username: %s", authentication.user)
}

var PROVENANCE_TYPE_TO_STRINGIFY_FUNC = map[ProvenanceType]func(Field) string{
	ProvenanceTypeConnection:     provenanceConnectionStringify,
	ProvenanceTypeAuthentication: provenanceAuthenticationStringify,
}

func deserializeConnectionProvenance(data []byte) ProvenanceConnection {
	assert(len(data) == 4, "invalid data length")

	ipv4 := binary.LittleEndian.Uint32(data[:4])

	return ProvenanceConnection{ipv4: ipv4}
}

func provenanceConnectionScore(field Field) ProvenanceScore {
	blobField, ok := field.(BlobField)
	assert(ok, "failed to retrieve int field from data reliability provenance")

	connectionProv := deserializeConnectionProvenance(blobField.Data)
	return connectionScore(connectionProv)
}

func provenanceAuthenticationScore(field Field) ProvenanceScore {
	blobField, ok := field.(BlobField)
	assert(ok, "failed to retrieve blob field from authentication provenance")
	authentication := deserializeProvenanceAuthenticationField(blobField.Data)

	return authenticationScore(authentication)
}

var PROVENANCE_TYPE_TO_SCORE_FUNC = map[ProvenanceType]func(Field) ProvenanceScore{
	ProvenanceTypeConnection:     provenanceConnectionScore,
	ProvenanceTypeAuthentication: provenanceAuthenticationScore,
}

func deserializeProvenanceAuthentication(data []byte) ProvenanceField {
	return ProvenanceField{Type: ProvenanceTypeAuthentication, Value: BlobField{Data: data}}
}

func deserializeProvenanceConnection(data []byte) ProvenanceField {
	assert(len(data) == 4, "invalid connection provenance size")

	return ProvenanceField{Type: ProvenanceTypeConnection, Value: BlobField{Data: data}}
}

var PROVENANCE_TYPE_TO_DESERIALIZATION_FUNC = map[ProvenanceType]func([]byte) ProvenanceField{
	ProvenanceTypeConnection:     deserializeProvenanceConnection,
	ProvenanceTypeAuthentication: deserializeProvenanceAuthentication,
}

var AMOUNT_OF_PROVENANCE_COLUMNS int = len(PROVENANCE_TYPE_TO_DESERIALIZATION_FUNC)

type ProvenanceAuthentication struct {
	user     string
	password string
}

type ProvenanceConnection struct {
	ipv4 uint32
}

type ProvenanceField struct {
	Type     ProvenanceType
	Value    Field
	operator ProvenanceOperator
	operands []*ProvenanceField
}

func (field ProvenanceField) getType() FieldType {
	return FieldTypeProvenance
}

func deserializeProvenanceField(data []byte) Field {
	provType := binary.LittleEndian.Uint16(data[:2])
	operator := ProvenanceOperator(data[2])

	// pass the data past the type
	field := PROVENANCE_TYPE_TO_DESERIALIZATION_FUNC[ProvenanceType(provType)](data[3:])
	field.operator = operator
	return field
}

func (field ProvenanceField) serialize() []byte {
	assert(field.operator == ProvenanceOperatorNil, "complex provenances' serialize is not implemented")

	fieldData := field.Value.serialize()
	data := make([]byte, 3)
	binary.LittleEndian.PutUint16(data, uint16(field.Type))
	data[2] = byte(field.operator)
	data = append(data, fieldData...)

	return data
}

func (field ProvenanceField) Stringify() string {
	assert(field.operator == ProvenanceOperatorNil, "complex provenances' stringify is not implemented")

	return PROVENANCE_TYPE_TO_STRINGIFY_FUNC[field.Type](field.Value)
}

func (field ProvenanceField) ToKey() *b_tree.BTreeKeyType {
	score := PROVENANCE_TYPE_TO_SCORE_FUNC[field.Type](field.Value)
	key := b_tree.BTreeKeyType(int(score))
	return &key
}

func addProvenanceToRecord(db *openDB, record *Record) {
	record.Provenance = db.provFields
}

func provenanceGenerateConnectionField(db *openDB) ProvenanceField {
	connectionData := serializeProvenanceConnectionField(db.connection)
	connectionField := BlobField{Data: connectionData}
	return ProvenanceField{Type: ProvenanceTypeConnection, Value: connectionField,
		operator: ProvenanceOperatorNil}
}

func provenanceGenerateAuthenticationField(db *openDB) ProvenanceField {
	authData := serializeProvenanceAuthenticationField(db.authentication)
	authField := BlobField{Data: authData}
	return ProvenanceField{Type: ProvenanceTypeAuthentication, Value: authField,
		operator: ProvenanceOperatorNil}
}

var PROV_COL_NAME_TO_FIELD_GENERATOR = map[string]func(db *openDB) ProvenanceField{
	"__PROV_CONNECTION__":     provenanceGenerateConnectionField,
	"__PROV_AUTHENTICATION__": provenanceGenerateAuthenticationField,
}

// returns the column headers of the configured provenance fields
func (db *openDB) provenanceSchemeColumns() []columnHeader {
	if !db.header.provenanceOn {
		return make([]columnHeader, 0)
	}

	cols := []columnHeader{
		{columnName: "__PROV_CONNECTION__", columnType: FieldTypeProvenance},
		{columnName: "__PROV_AUTHENTICATION__", columnType: FieldTypeProvenance}}

	return cols
}

func generateOpenDBProvenance(db *openDB) []ProvenanceField {
	provFields := make([]ProvenanceField, 0)
	for _, column := range db.provenanceSchemeColumns() {
		provField := PROV_COL_NAME_TO_FIELD_GENERATOR[column.columnName](db)
		provFields = append(provFields, provField)
	}

	return provFields
}

type OpenDBProvenance struct {
	auth ProvenanceAuthentication
	conn ProvenanceConnection
}

func provenanceApplyOperatorToProvenanceList(provenances []ProvenanceField, operator ProvenanceOperator) []ProvenanceField {
	provenanceByType := make(map[ProvenanceType][]*ProvenanceField)
	for _, provenance := range provenances {
		fields, ok := provenanceByType[provenance.Type]
		if !ok {
			fields = make([]*ProvenanceField, 0)
		}
		fields = append(fields, &provenance)

		provenanceByType[provenance.Type] = fields
	}

	fields := make([]ProvenanceField, 0)
	for provType := range provenanceByType {
		field := ProvenanceField{operator: operator, operands: provenanceByType[provType],
			Type: provType}
		fields = append(fields, field)
	}

	return fields
}

// retrieves the provenance fields from all records, for each provenance type
// performs the multiplication operator on all the fields, then returns the
// resulting provenance fields
func provenanceApplyJoin(record *jointRecord) {
	assert(len(record.offsets) > 0, "empty joint record is not supported")

	if len(record.offsets) == 1 {
		return
	}

	jointProvenance := record.record.Provenance
	record.record.Provenance =
		provenanceApplyOperatorToProvenanceList(jointProvenance, ProvenanceOperatorMultiply)
}

func provenanceApplySelect(identicalRecords []Record) Record {
	assert(len(identicalRecords) > 0, "empty identical records list is not supported")

	// if there is only one record with those values, don't apply any change to its provenance
	if len(identicalRecords) == 1 {
		return identicalRecords[0]
	}

	projectedRecord := Record{Fields: identicalRecords[0].Fields, Provenance: make([]ProvenanceField, 0)}
	for _, record := range identicalRecords {
		projectedRecord.Provenance = append(projectedRecord.Provenance, record.Provenance...)
	}

	projectedRecord.Provenance =
		provenanceApplyOperatorToProvenanceList(projectedRecord.Provenance, ProvenanceOperatorPlus)
	return projectedRecord
}
