package TdNeo

import (
	"database/sql/driver"
	"fmt"
	"github.com/simonalong/gole/logger"
	"github.com/simonalong/gole/util"
	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/types"
	"strings"
	"tdNeo/neomap"
)

type TdNeo struct {
	// 超级表中的每个属性对应的类型关系，其中key：stableName value:key，fieldName/tagName；value：taosType
	StableFieldTypeMap map[string]map[string]TdengineFieldMeta
	// 子表对应的超级表map
	TableStableMap map[string]string
	// 数据库链接
	TdConnect *af.Connector
	// 数据库名：我们这里一个库对应一个对象
	DbName string
}

type TdengineFieldMeta struct {
	// taos类型：例如：types.TaosBoolType
	ColType interface{}
	// 类型长度
	ColLen int
}

func NewConnect(host, user, pass, dbName string, port int) *TdNeo {
	conn, err := af.Open(host, user, pass, dbName, port)
	if err != nil {
		logger.Error("tdengine连接异常，请检查配置 %v", err.Error())
		return nil
	}
	pNeo := &TdNeo{
		StableFieldTypeMap: make(map[string]map[string]TdengineFieldMeta),
		TdConnect:          conn,
		DbName:             dbName,
	}

	pNeo.loadStableFieldOfDb()
	pNeo.loadTableStableMapOfDb()
	return pNeo
}

func (neo *TdNeo) Exec(sql string) (driver.Result, error) {
	return neo.TdConnect.Exec(sql)
}

func (neo *TdNeo) Insert(tableName string, dataMap *neomap.NeoMap) (driver.Result, error) {
	if dataMap.IsEmpty() {
		logger.Warn("insert 数据为空")
		return nil, nil
	}
	dataMap.SetSort(true)
	sql := generateInsertSql(tableName, dataMap)
	param := generateInsertParams(neo, tableName, dataMap)
	logger.Group("sql").Debugf("sql ==> %v", sql)
	return neo.TdConnect.StmtExecute(sql, param)
}

func (neo *TdNeo) InsertEntity(tableName string, entity interface{}) (driver.Result, error) {
	if entity == nil {
		logger.Warn("insert entity 数据为空")
		return nil, nil
	}

	return neo.Insert(tableName, neomap.From(entity))
}

func generateInsertSql(tableName string, dataMap *neomap.NeoMap) string {
	return "insert into " + tableName + "(" + generateFieldsSql(dataMap) + ") values(" + generateFieldsSeizeSql(dataMap) + ")"
}

func generateInsertParams(neo *TdNeo, tableName string, dataMap *neomap.NeoMap) *param.Param {
	len := dataMap.Size()
	finalParam := param.NewParam(dataMap.Size())
	fieldMap := neo.getTableFiledTaosTypeMap(tableName)
	for i, key := range dataMap.Keys() {
		if i >= len {
			break
		}
		fieldMeta := fieldMap[key]
		switch fieldMeta.ColType {
		case types.TaosBoolType:
			finalParam.AddBool(dataMap.GetBool(key))
		case types.TaosTinyintType:
			finalParam.AddTinyint(dataMap.GetInt(key))
		case types.TaosSmallintType:
			finalParam.AddSmallint(dataMap.GetInt(key))
		case types.TaosIntType:
			finalParam.AddInt(dataMap.GetInt(key))
		case types.TaosBigintType:
			finalParam.AddBigint(dataMap.GetInt(key))
		case types.TaosUTinyintType:
			finalParam.AddUTinyint(dataMap.GetUInt(key))
		case types.TaosUSmallintType:
			finalParam.AddUSmallint(dataMap.GetUInt(key))
		case types.TaosUIntType:
			finalParam.AddUInt(dataMap.GetUInt(key))
		case types.TaosUBigintType:
			finalParam.AddUBigint(dataMap.GetUInt(key))
		case types.TaosFloatType:
			finalParam.AddFloat(dataMap.GetFloat32(key))
		case types.TaosDoubleType:
			finalParam.AddDouble(dataMap.GetFloat64(key))
		case types.TaosBinaryType:
			finalParam.AddBinary(dataMap.GetBytes(key))
		case types.TaosVarBinaryType:
			finalParam.AddVarBinary(dataMap.GetBytes(key))
		case types.TaosNcharType:
			finalParam.AddNchar(dataMap.GetString(key))
		case types.TaosTimestampType:
			finalParam.AddTimestamp(dataMap.GetTime(key), 0)
		case types.TaosJsonType:
			finalParam.AddJson(dataMap.GetBytes(key))
		case types.TaosGeometryType:
			finalParam.AddGeometry(dataMap.GetBytes(key))
		}
	}
	return finalParam
}

// 返回：`ts`, `name`, `age`
func generateFieldsSql(dataMap *neomap.NeoMap) string {
	var keys []string
	for _, key := range dataMap.Keys() {
		if !strings.HasPrefix(key, "`") && !strings.HasSuffix(key, "`") {
			keys = append(keys, "`"+key+"`")
		}
	}
	return strings.Join(keys, ",")
}

// 返回：?, ?, ?
func generateFieldsSeizeSql(dataMap *neomap.NeoMap) string {
	var seizes []string
	for range dataMap.Keys() {
		seizes = append(seizes, "?")
	}
	return strings.Join(seizes, ",")
}

func (neo *TdNeo) loadStableFieldOfDb() {
	conn := neo.TdConnect
	dbName := neo.DbName
	neo.StableFieldTypeMap = make(map[string]map[string]TdengineFieldMeta)
	rows, err := conn.Query(fmt.Sprintf("select `table_name`,`col_name`,`col_type`,`col_length` from information_schema.ins_columns where `db_name` = '%s' and (`table_type` = \"SUPER_TABLE\" or `table_type`=\"NORMAL_TABLE\")", dbName))
	if err != nil {
		logger.Error("获取数据库的元数据异常：%v", err.Error())
		return
	}
	dest := make([]driver.Value, 4)
	fieldMap := map[string]TdengineFieldMeta{}
	var lastStableName string
	for rows.Next(dest) == nil {
		currentStableName := util.ToString(dest[0])
		if lastStableName != "" && lastStableName != currentStableName {
			neo.StableFieldTypeMap[lastStableName] = fieldMap
			fieldMap = make(map[string]TdengineFieldMeta)
		}
		lastStableName = currentStableName
		fieldMap[util.ToString(dest[1])] = TdengineFieldMeta{
			ColType: tdengineColTypeToTaosType(util.ToString(dest[2])),
			ColLen:  util.ToInt(dest[3]),
		}
	}
	neo.StableFieldTypeMap[lastStableName] = fieldMap
	fieldMap = make(map[string]TdengineFieldMeta)
}

func (neo *TdNeo) loadTableStableMapOfDb() {
	conn := neo.TdConnect
	dbName := neo.DbName
	neo.TableStableMap = make(map[string]string)
	rows, err := conn.Query(fmt.Sprintf("select `table_name`, `stable_name` from information_schema.ins_tables where db_name= '%s'", dbName))
	if err != nil {
		logger.Error("获取数据库的元数据子表和超表关系异常：%v", err.Error())
		return
	}
	dest := make([]driver.Value, 2)

	tableStableMap := map[string]string{}
	for rows.Next(dest) == nil {
		tableName := util.ToString(dest[0])
		stableName := util.ToString(dest[1])
		tableStableMap[tableName] = stableName
	}
	neo.TableStableMap = tableStableMap
}

func (neo *TdNeo) getTableFiledTaosTypeMap(tableName string) map[string]TdengineFieldMeta {
	if _, exit := neo.TableStableMap[tableName]; !exit {
		neo.loadTableStableMapOfDb()
	}

	var finalTableName string
	stableName, exit := neo.TableStableMap[tableName]
	if !exit {
		logger.Error("当前表不存在")
		return nil
	}
	if stableName == "" {
		finalTableName = tableName
	} else {
		finalTableName = stableName
	}

	fieldMap := neo.StableFieldTypeMap[finalTableName]
	return fieldMap
}

// tdengine的数据库类型向taos类型转换
func tdengineColTypeToTaosType(colTypeStr string) interface{} {
	switch colTypeStr {
	case "TIMESTAMP":
		return types.TaosTimestampType
	case "BOOL":
		return types.TaosBoolType
	case "TINYINT":
		return types.TaosTinyintType
	case "SMALLINT":
		return types.TaosSmallintType
	case "INT":
		return types.TaosIntType
	case "BIGINT":
		return types.TaosBigintType
	case "TINYINT UNSIGNED":
		return types.TaosUTinyintType
	case "SMALLINT UNSIGNED":
		return types.TaosUSmallintType
	case "INT UNSIGNED":
		return types.TaosUIntType
	case "BIGINT UNSIGNED":
		return types.TaosUBigintType
	case "FLOAT":
		return types.TaosFloatType
	case "DOUBLE":
		return types.TaosDoubleType
	case "VARBINARY":
		return types.TaosVarBinaryType
	case "GEOMETRY":
		return types.TaosGeometryType
	}

	if strings.HasPrefix(colTypeStr, "VARCHAR") {
		return types.TaosBinaryType
	} else if strings.HasPrefix(colTypeStr, "NCHAR") {
		return types.TaosNcharType
	}
	return nil
}
