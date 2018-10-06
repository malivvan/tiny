package tiny

import (
	"testing"
	"os"
	"reflect"
)

func TestMapBasic(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove("test.db")
	}()
	m, err := db.Store().OpenMap("map", ModeMem, TestStruct{})
	if err != nil {
		t.Fatal(err)
	}

	// insert struct pointer, check if get returns the same address
	ts := newTestStruct()
	err = m.Set("key", ts)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := m.Get("key")
	if ts2 == nil {
		t.Fatal()
	}
	if ts != ts2 {
		t.Fail()
	}

	// reopen and check if test struct values equal
	db.Close()
	db, err = Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	m, err = db.Store().OpenMap("map", ModeMem, TestStruct{})
	if err != nil {
		t.Fatal(err)
	}
	ts3 := m.Get("key")
	if ts3 == nil {
		t.Fatal()
	}
	if ts2 == ts3 {
		t.Fatal("fail")
	}
	if !reflect.DeepEqual(ts2, ts3) {
		t.Fatal("ts2 should equal ts3")
	}

	// remove key and check if it stays gone after database reload
	err = m.Set("key", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Get("key") != nil {
		t.Fatal("expected nil value")
	}
	db.Close()
	db, err = Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	m, err = db.Store().OpenMap("map", ModeMem, TestStruct{})
	if err != nil {
		t.Fatal(err)
	}
	if m.Get("key") != nil {
		t.Fatal("expected nil value")
	}

	// open submap in disk mode, insert teststruct, read twice and check for different pointers (because of disk mode)
	// and equal values
	m2, err := m.Store().OpenMap("submap", ModeDisk, TestStruct{})
	if err != nil {
		t.Fatal(err)
	}
	err = m2.Set("submapkey", ts)
	if err != nil {
		t.Fatal(err)
	}
	ts4 := m2.Get("submapkey")
	if ts4 == nil {
		t.Fatal()
	}
	ts5 := m2.Get("submapkey")
	if ts5 == nil {
		t.Fatal()
	}
	if ts4 == ts5 {
		t.Fatal("ts4 should have a different address than ts5")
	}
	if !reflect.DeepEqual(ts4, ts5) {
		t.Fatal("ts4 does not equal ts5")
	}

	// test some list outcomes
	dbList, err := db.Store().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbList) != 1 || dbList[0] != "map" {
		t.Fatal("expected single element slice with element 'map'")
	}
	mList, err := m.Store().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(mList) != 1 || mList[0] != "submap" {
		t.Fatal("expected single element slice with element 'submap'")
	}

	// remove parent map, reopen and check outcome
	err = db.Store().Remove("map")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	dbList, err = db.Store().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbList) != 0 {
		t.Fatal("expected empty slice")
	}
	if db.Store().Remove("map").Error() != "store 'map' not found" {
		t.Fatal()
	}
}
