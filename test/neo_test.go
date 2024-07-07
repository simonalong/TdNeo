package test

import (
	"fmt"
	"github.com/simonalong/gole/time"
	"tdNeo"
	"tdNeo/neomap"
	"testing"
)

func TestConnect(t *testing.T) {
	host := "localhost"
	port := 6030
	user := "root"
	password := "taosdata"
	db := "tdlearn"

	// 连接
	tdNeo := TdNeo.NewConnect(host, user, password, db, port)

	// 建超级表
	_, err := tdNeo.Exec("create stable if not exists neo_demo1(ts timestamp, name nchar(32), age int, address nchar(128)) tags (station nchar(128))")
	checkErr(err, "建超级表失败")

	// 建子表
	_, err = tdNeo.Exec("create table if not exists neo_china using neo_demo1(`station`) tags(\"china\")")
	checkErr(err, "建子表失败")

	// 新增：使用map
	insertMap := neomap.New()
	insertMap.SetSort(true)
	insertMap.Put("ts", time.Now())
	insertMap.Put("name", "大牛市")
	insertMap.Put("age", "18")
	insertMap.Put("address", "浙江杭州市")
	_, err = tdNeo.Insert("neo_china", insertMap)
	checkErr(err, "插入数据")

	// 新增：使用entity
	neoChinaDomain := NeoChinaDomain{
		Ts:      time.Now(),
		Name:    "大牛市2",
		Age:     19,
		Address: "浙江温州市",
	}
	_, err = tdNeo.InsertEntity("neo_china", neoChinaDomain)
	checkErr(err, "插入数据")

	// 删除

	// 修改

	// 查询：一行

	// 查询：多行

	// 查询：一列

	// 查询：多列

	// 新增：批量

}

func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Printf("%s\n", prompt)
		panic(err)
	}
}
