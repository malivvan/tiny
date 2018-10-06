package tiny

import (
	"time"
)

type TestStruct struct {
	String string
	Int    int
	Float  float64
	Time   time.Time
	Map    map[string]string
	Slice  []float64
}

func newTestStruct() *TestStruct {
	return &TestStruct{
		String: "aösldkaödl",
		Int:    23123123123,
		Float:  1231231.31231233,
		Time:   time.Now().Round(time.Nanosecond),
		Map:    map[string]string{"asdas": "adas"},
		Slice:  []float64{1213123.123123, 123.1231313123, 5555.33333333},
	}
}
