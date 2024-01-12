package Log

import (
	"fmt"
	"testing"
)

func TestLogToString(t *testing.T) {
	x := Log{
		K: Key{-1, -1},
		V: "cbuiabva b  boabobvab    ubaacbai  jadbabdaibtim",
	}
	fmt.Println(LogToString(x))
	fmt.Println(StringToLog(LogToString(x)))
}
