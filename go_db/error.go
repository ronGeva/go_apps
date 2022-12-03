package go_db

import "strconv"

type UnsupportedError struct {
}

func (err *UnsupportedError) Error() string {
	return "A call to an unsupported operation was performed!"
}

type MalformedDBError struct {
}

func (err *MalformedDBError) Error() string {
	return "A malformed database was encountered, cannot continue"
}

type ReadError struct {
}

func (err *ReadError) Error() string {
	return "A read has returned with faulty size"
}

type BadReadRequestError struct {
}

func (err *BadReadRequestError) Error() string {
	return "A read request was performed with impossible parameters"
}

type DeserializationError struct {
}

func (err *DeserializationError) Error() string {
	return "Failed to deserialize an object"
}

type InsufficientWriteError struct {
	expectedSize uint32
	actualSize   int
}

func (err *InsufficientWriteError) Error() string {
	return "Write error, expected to write " + strconv.FormatUint(uint64(err.expectedSize), 10) +
		" bytes, instead only written " + strconv.FormatUint(uint64(err.actualSize), 10)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type AssertionError struct {
	assertionMessage string
}

func (err *AssertionError) Error() string {
	return err.assertionMessage
}

type TableNotFoundError struct {
	identifier string
}

func (err *TableNotFoundError) Error() string {
	return "Failed to find table with ID " + err.identifier
}

type RecordNotFoundError struct {
}

func (err *RecordNotFoundError) Error() string {
	return "Record was not found"
}

func assert(b bool, msg string) {
	if !b {
		panic(AssertionError{msg})
	}
}
