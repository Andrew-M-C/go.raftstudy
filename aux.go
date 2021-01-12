package main

import (
	"fmt"
	"time"

	"github.com/Andrew-M-C/go-tools/callstack"
)

var startTime = time.Now()

func prefix() string {
	t := time.Now().Sub(startTime).Truncate(time.Microsecond)
	fi, li, _ := callstack.CallerInfo(2)
	return fmt.Sprintf("%s, Line %d, %s - ", fi, li, t.String())
}

func infof(f string, a ...interface{}) {
	a = append([]interface{}{prefix()}, a...)
	fmt.Printf("%s - "+f+"\n", a...)
}

func errorf(f string, a ...interface{}) {
	a = append([]interface{}{prefix()}, a...)
	fmt.Printf("%s - "+f+"\n", a...)
}
