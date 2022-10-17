package main

import (
	"fmt"
	"regex_engine"
)

func main() {
	res := regex_engine.CompilePattern("Hello world")
	fmt.Println(res.Match("Hello worldd"))
}
