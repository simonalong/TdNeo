package neomap

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/simonalong/gole/logger"
	"github.com/simonalong/gole/util"
	"reflect"
	"strings"
	"time"
)

/**
 * 提供新的map
 * 1. 提供类型转换
 * 2. 并发安全
 * 3. 提供有序性
 * 4. 提供与实体的转化功能
 */

type NeoMap struct {
	innerMap cmap.ConcurrentMap
	sort     bool
	keys     []string
}

func New() *NeoMap {
	return &NeoMap{
		innerMap: cmap.New(),
		sort:     false,
		keys:     make([]string, 0),
	}
}

// From 默认从实体这边转移过来的，默认为优秀
func From(entity interface{}) *NeoMap {
	if entity == nil {
		return nil
	}
	objType := reflect.TypeOf(entity)
	objValue := reflect.ValueOf(entity)
	// 只接收结构体类型
	if objType.Kind() != reflect.Struct {
		return nil
	}

	entityMap := &NeoMap{
		innerMap: cmap.New(),
		sort:     true,
		keys:     make([]string, 0),
	}
	for fieldIndex, num := 0, objType.NumField(); fieldIndex < num; fieldIndex++ {
		field := objType.Field(fieldIndex)
		if !util.IsPublic(field.Name) {
			continue
		}

		columnName := getFinalColumnName(field)

		fieldValue := objValue.Field(fieldIndex)
		entityMap.Put(columnName, fieldValue.Interface())
	}
	return entityMap
}

// To 将数据输出到实体中
func (receiver *NeoMap) To(pEntity interface{}) interface{} {
	if receiver.IsEmpty() {
		return nil
	}
	targetType := reflect.TypeOf(pEntity)
	if targetType.Kind() != reflect.Ptr {
		logger.Warn("接收的实体必须为指针类型")
		return nil
	}

	if targetType.Elem().Kind() != reflect.Struct {
		logger.Warn("接收的实体指针类型必须指向的是实体类型")
		return nil
	}

	targetValue := reflect.ValueOf(pEntity)
	for index, num := 0, targetType.Elem().NumField(); index < num; index++ {
		field := targetType.Elem().Field(index)
		fieldValue := targetValue.Elem().Field(index)

		// 私有字段不处理
		if util.IsPrivate(field.Name) {
			continue
		}

		doInvokeValue(receiver, reflect.ValueOf(dataMap), field, fieldValue)
	}
}

func doInvokeValue(receiver *NeoMap, fieldMapValue reflect.Value, field reflect.StructField, fieldValue reflect.Value) {
	if fieldMapValue.Kind() == reflect.Ptr {
		fieldMapValue = fieldMapValue.Elem()
	}

	var fValue reflect.Value
	columnName := getFinalColumnName(field)
	if v, exist := receiver.Get(columnName); exist {
		reflect.ValueOf(v)
	}

	// todo
	//
	//if fieldValue.Kind() == reflect.Ptr {
	//	fValue = fValue.Elem()
	//}
	//targetValue := valueToTarget(fValue, field.Type)
	//if targetValue.IsValid() {
	//	if fieldValue.Kind() == reflect.Ptr {
	//		if targetValue.Kind() == reflect.Ptr {
	//			fieldValue.Elem().FieldByName(field.Name).Set(targetValue.Elem().Convert(field.Type))
	//		} else {
	//			fieldValue.Elem().FieldByName(field.Name).Set(targetValue.Convert(field.Type))
	//		}
	//	} else {
	//		if targetValue.Kind() == reflect.Ptr {
	//			fieldValue.Set(targetValue.Elem().Convert(field.Type))
	//		} else {
	//			fieldValue.Set(targetValue.Convert(field.Type))
	//		}
	//	}
	//}
}

func valueToTarget(srcValue reflect.Value, dstType reflect.Type) reflect.Value {
	if dstType.Kind() == reflect.Struct {
		if srcValue.Kind() == reflect.Ptr {
			srcValue = srcValue.Elem()
		}
		sourceValue := reflect.ValueOf(srcValue.Interface())
		if sourceValue.Kind() == reflect.Map || sourceValue.Kind() == reflect.Struct {
			mapFieldValue := reflect.New(dstType)
			for index, num := 0, mapFieldValue.Type().Elem().NumField(); index < num; index++ {
				field := mapFieldValue.Type().Elem().Field(index)
				fieldValue := mapFieldValue.Elem().Field(index)

				doInvokeValue(sourceValue, field, fieldValue)
			}
			return mapFieldValue
		}
	} else if dstType.Kind() == reflect.Map {
		if srcValue.Kind() == reflect.Ptr {
			srcValue = srcValue.Elem()
		}
		sourceValue := reflect.ValueOf(srcValue.Interface())
		if sourceValue.Kind() == reflect.Map {
			mapFieldValue := reflect.MakeMap(dstType)
			for mapR := sourceValue.MapRange(); mapR.Next(); {
				mapKey := mapR.Key()
				mapValue := mapR.Value()

				mapKeyRealValue, err := Cast(mapFieldValue.Type().Key().Kind(), fmt.Sprintf("%v", mapKey.Interface()))
				mapValueRealValue := valueToTarget(mapValue, mapFieldValue.Type().Elem())
				if err == nil {
					if mapValueRealValue.Kind() == reflect.Ptr {
						mapFieldValue.SetMapIndex(reflect.ValueOf(mapKeyRealValue), mapValueRealValue.Elem())
					} else {
						mapFieldValue.SetMapIndex(reflect.ValueOf(mapKeyRealValue), mapValueRealValue)
					}
				}
			}
			return mapFieldValue
		} else if sourceValue.Kind() == reflect.Struct {
			srcType := reflect.TypeOf(sourceValue)
			srcValue := reflect.ValueOf(sourceValue)
			mapFieldValue := reflect.MakeMap(dstType)

			for index, num := 0, srcType.NumField(); index < num; index++ {
				field := srcType.Field(index)
				fieldValue := srcValue.Field(index)

				mapValueRealValue := ObjectToData(fieldValue.Interface())
				mapFieldValue.SetMapIndex(reflect.ValueOf(ToLowerFirstPrefix(field.Name)), reflect.ValueOf(mapValueRealValue))

				doInvokeValue(sourceValue, field, fieldValue)
			}
			return mapFieldValue
		}
	} else if dstType.Kind() == reflect.Slice || dstType.Kind() == reflect.Array {
		if srcValue.Kind() == reflect.Ptr {
			srcValue = srcValue.Elem()
		}
		sourceValue := reflect.ValueOf(srcValue.Interface())
		if sourceValue.Kind() == reflect.Slice || sourceValue.Kind() == reflect.Array {
			arrayFieldValue := reflect.MakeSlice(dstType, 0, 0)
			for arrayIndex := 0; arrayIndex < sourceValue.Len(); arrayIndex++ {
				dataV := valueToTarget(sourceValue.Index(arrayIndex), dstType.Elem())
				if dataV.IsValid() {
					if dataV.Kind() == reflect.Ptr {
						arrayFieldValue = reflect.Append(arrayFieldValue, dataV.Elem())
					} else {
						arrayFieldValue = reflect.Append(arrayFieldValue, dataV)
					}
				}
			}
			return arrayFieldValue
		}
	} else if IsBaseType(dstType) {
		sourceValue := reflect.ValueOf(srcValue.Interface())
		if sourceValue.IsValid() && IsBaseType(sourceValue.Type()) {
			v, err := Cast(dstType.Kind(), fmt.Sprintf("%v", srcValue.Interface()))
			if err == nil {
				return reflect.ValueOf(v)
			}
		}
	} else if dstType.Kind() == reflect.Interface {
		return reflect.ValueOf(ObjectToData(srcValue.Interface()))
	} else if dstType.Kind() == reflect.Ptr {
		return srcValue
	} else {
		v, err := Cast(dstType.Kind(), fmt.Sprintf("%v", srcValue.Interface()))
		if err == nil {
			return reflect.ValueOf(v)
		}
	}
	return reflect.ValueOf(nil)
}

func getValueFromMapValue(keyValues reflect.Value, key string) (reflect.Value, bool) {
	if key == "" {
		return reflect.ValueOf(nil), false
	}
	if keyValues.Kind() == reflect.Map {
		if v1 := keyValues.MapIndex(reflect.ValueOf(key)); v1.IsValid() {
			return v1, true
		} else if v2 := keyValues.MapIndex(reflect.ValueOf(ToLowerFirstPrefix(key))); v2.IsValid() {
			return v2, true
		}
	}

	return reflect.ValueOf(nil), false
}

func (receiver *NeoMap) Keys() []string {
	if receiver.sort {
		return receiver.keys
	} else {
		return receiver.innerMap.Keys()
	}
}

// SetSort 设置map为有序或者无序map
// 注意：
//  1. 如果从无序变为有序，且之前已经有一些数据，则之前的数据顺序至此固定，后续的顺序就按照添加的顺序固定
//  2. 如果从有序变为无序，且之前已经有一些数据，则顺序就完全乱掉了
func (receiver *NeoMap) SetSort(sort bool) {
	if !receiver.sort && sort {
		receiver.keys = receiver.innerMap.Keys()
	} else if receiver.sort && !sort {
		receiver.keys = make([]string, 0)
	}
	receiver.sort = sort
}

func (receiver *NeoMap) IsEmpty() bool {
	return len(receiver.innerMap.Keys()) == 0
}

func (receiver *NeoMap) IsUnEmpty() bool {
	return len(receiver.innerMap.Keys()) != 0
}

func (receiver *NeoMap) Put(key string, value interface{}) {
	receiver.innerMap.Set(key, value)
	if receiver.sort {
		receiver.keys = append(receiver.keys, key)
	}
}

func (receiver *NeoMap) Get(key string) (interface{}, bool) {
	return receiver.innerMap.Get(key)
}

func (receiver *NeoMap) Contain(key string) bool {
	_, exit := receiver.innerMap.Get(key)
	return exit
}

func (receiver *NeoMap) GetWithExist(key string) (interface{}, bool) {
	return receiver.innerMap.Get(key)
}

func (receiver *NeoMap) GetInt(key string) int {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToInt(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetInt8(key string) int8 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToInt8(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetInt16(key string) int16 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToInt16(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetInt32(key string) int32 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToInt32(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetInt64(key string) int64 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToInt64(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetUInt(key string) uint {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToUInt(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetUInt8(key string) uint8 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToUInt8(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetUInt16(key string) uint16 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToUInt16(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetUInt32(key string) uint32 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToUInt32(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetUInt64(key string) uint64 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToUInt64(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetFloat32(key string) float32 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToFloat32(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetFloat64(key string) float64 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToFloat64(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetBool(key string) bool {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToBool(d)
	} else {
		return false
	}
}

func (receiver *NeoMap) GetComplex64(key string) complex64 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToComplex64(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetComplex128(key string) complex128 {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToComplex128(d)
	} else {
		return 0
	}
}

func (receiver *NeoMap) GetString(key string) string {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return util.ToString(d)
	} else {
		return ""
	}
}

func (receiver *NeoMap) GetTime(key string) time.Time {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return d.(time.Time)
	} else {
		logger.Warn("map中的key（%v）获取time不存在", key)
		return time.Now()
	}
}

func (receiver *NeoMap) GetBytes(key string) []byte {
	d, exit := receiver.innerMap.Get(key)
	if exit {
		return []byte(util.ToString(d))
	} else {
		return nil
	}
}

func (receiver *NeoMap) Remove(key string) {
	receiver.innerMap.Remove(key)
	if receiver.sort {
		id := util.IndexOf(receiver.keys, key)
		receiver.keys = append(receiver.keys[:id], receiver.keys[id+1:]...)
	}
}
func (receiver *NeoMap) RemoveAll() {
	receiver.innerMap.Clear()
	if receiver.sort {
		receiver.keys = make([]string, 0)
	}
}
func (receiver *NeoMap) Clear() {
	receiver.innerMap.Clear()
	if receiver.sort {
		receiver.keys = make([]string, 0)
	}
}
func (receiver *NeoMap) Size() int {
	return len(receiver.innerMap.Keys())
}

func getFinalColumnName(field reflect.StructField) string {
	columnName := field.Tag.Get("column")
	if len(columnName) != 0 {
		return columnName
	}

	// 如果没有配置column标签，也可以使用json标签，这里也支持
	aliasJson := field.Tag.Get("json")
	index := strings.Index(aliasJson, ",")
	if index != -1 {
		return aliasJson[:index]
	}

	// 如果也没有配置json标签，则使用属性的属性名，将首字母变小写
	return util.ToLowerFirstPrefix(field.Name)
}
