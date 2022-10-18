package main

import (
	"fmt"
	"regex_engine"
)

func testWithPlus() {
	res := regex_engine.CompilePattern("Hello w+world+a")
	fmt.Println(res.Match("Hello world"))
	fmt.Println(res.Match("Hello wwwwworldd"))
	fmt.Println(res.Match("Hello orldd"))
	fmt.Println(res.Match("Hello wworldddddaa"))
	fmt.Println(res.Match("Hello wworl"))
	fmt.Println(res.Match("Hello wwwworldddda"))
}

func testJoker() {
	res := regex_engine.CompilePattern("Hello w.rld")
	fmt.Println("true=", res.Match("Hello world"))
	fmt.Println("true=", res.Match("Hello wwrld"))
	fmt.Println("false=", res.Match("Hello wadrld"))
	fmt.Println("false=", res.Match("Hello ld"))

	res = regex_engine.CompilePattern("Hello w.+rld")
	fmt.Println("true=", res.Match("Hello world"))
	fmt.Println("true=", res.Match("Hello wwrld"))
	fmt.Println("true=", res.Match("Hello wadrld"))
	fmt.Println("false=", res.Match("Hello ld"))
	fmt.Println("true=", res.Match("Hello waksdjaskjdrld"))

	res = regex_engine.CompilePattern("H.llo wo.+ld.+abba")
	fmt.Println("true=", res.Match("Hello worldaskdkasdjabba"))
	fmt.Println("false=", res.Match("Hello woldabba"))
}

func testSimple() {
	res := regex_engine.CompilePattern("Hello world")
	fmt.Println(res.Match("Hello worldd"))
}

func main() {
	regex := regex_engine.CompilePattern("Hey shai.+krasin")
	fmt.Println(regex.Match("Hey shai the bo$$ krasin"))
	fmt.Println(regex.Match("Hey shai not the bo$$ krasin with faulty string"))
}
