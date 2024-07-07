package test

import (
	"fmt"
	"tdNeo/neomap"
	"testing"
	"time"
)

func TestPutGet(t *testing.T) {
	dataMap := neomap.New()
	dataMap.Put("a", 12)

	fmt.Println(dataMap.GetInt("a"))
}

func TestRange1(t *testing.T) {
	dataMap := neomap.New()
	dataMap.SetSort(true)
	dataMap.Put("a", 12)
	dataMap.Put("b", 13)
	dataMap.Put("c", 124)
	dataMap.Put("d", 54)
	dataMap.Put("e", 36)

	// 循环
	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

func TestRange2(t *testing.T) {
	dataMap := neomap.New()
	dataMap.Put("a", 12)
	dataMap.Put("b", 13)
	dataMap.Put("c", 124)

	dataMap.SetSort(true)
	dataMap.Put("d", 54)
	dataMap.Put("e", 36)

	// 循环
	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

func TestRange3(t *testing.T) {
	dataMap := neomap.New()
	dataMap.SetSort(true)
	dataMap.Put("a", 12)
	dataMap.Put("b", 13)
	dataMap.Put("c", 124)

	dataMap.SetSort(false)
	dataMap.Put("d", 54)
	dataMap.Put("e", 36)

	// 循环：请使用keys循环
	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

type DemoEntity1 struct {
	Ts      time.Time `column:"ts"`
	Name    string    `column:"name"`
	Age     int       `column:"age"`
	Address string    `column:"address"`
}

func TestFrom1(t *testing.T) {
	entity1 := DemoEntity1{
		Ts:      time.Now(),
		Name:    "test",
		Age:     22,
		Address: "浙江",
	}

	dataMap := neomap.From(entity1)

	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

type DemoEntity2 struct {
	Ts      time.Time `json:"ts"`
	Name    string    `json:"name"`
	Age     int       `json:"age"`
	Address string    `json:"address"`
}

func TestFrom2(t *testing.T) {
	entity2 := DemoEntity2{
		Ts:      time.Now(),
		Name:    "test",
		Age:     22,
		Address: "浙江",
	}

	dataMap := neomap.From(entity2)

	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

type DemoEntity3 struct {
	Ts      time.Time
	Name    string
	Age     int
	Address string
}

func TestFrom3(t *testing.T) {
	entity3 := DemoEntity3{
		Ts:      time.Now(),
		Name:    "test",
		Age:     22,
		Address: "浙江",
	}

	dataMap := neomap.From(entity3)

	for _, key := range dataMap.Keys() {
		val, _ := dataMap.Get(key)
		fmt.Println(key, val)
	}
}

func TestTo1(t *testing.T) {
	entity3 := DemoEntity3{
		Ts:      time.Now(),
		Name:    "test",
		Age:     22,
		Address: "浙江",
	}

	dataMap := neomap.From(entity3)

	dataMap.
}
