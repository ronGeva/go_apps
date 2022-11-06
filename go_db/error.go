package go_db

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
	return "Write error, expected to write " + string(err.expectedSize) +
		" bytes, instead only written " + string(err.actualSize)
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

func assert(b bool, msg string) {
	if !b {
		panic(AssertionError{msg})
	}
}
