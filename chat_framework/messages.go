package chat_framework

type HelloMessage struct {
	Header   [4]byte
	Greeting string
}

func (helloMsg *HelloMessage) Encode() []byte {
	data := make([]byte, 0)
	data = append(data, helloMsg.Header[:]...)
	data = append(data, []byte(helloMsg.Greeting)...)
	return data
}
