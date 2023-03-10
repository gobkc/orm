package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var allows = []reflect.Kind{reflect.Struct, reflect.Slice, reflect.Int, reflect.Int64, reflect.String, reflect.Float64}
var ErrAllow = fmt.Errorf("query: allow list: reflect.Struct/reflect.Slice/reflect.Int/reflect.Int64/reflect.String/reflect.Float64")
var ErrInsertAllow = fmt.Errorf("query: allow list: reflect.Struct")
var ErrUpdateAllow = ErrInsertAllow

func Query[T any](ctx context.Context, db *sql.DB, sqlStr string, args ...any) (t *T, err error) {
	t = new(T)
	sqlStr, args = parseSqlIn(sqlStr, args)
	defer outputSql(sqlStr, args)
	stmt, err := db.PrepareContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	kind := reflect.TypeOf(t).Elem().Kind()
	pass := false
	for _, allow := range allows {
		if allow == kind {
			pass = true
			break
		}
	}
	if !pass {
		return nil, ErrAllow
	}
	var unmarshalMap = map[reflect.Kind]func() error{
		reflect.Struct: func() error {
			return unmarshalStruct(rows, t)
		},
		reflect.Int: func() error {
			return unmarshalNumOrStr(rows, t)
		},
		reflect.Int64: func() error {
			return unmarshalNumOrStr(rows, t)
		},
		reflect.Float64: func() error {
			return unmarshalNumOrStr(rows, t)
		},
		reflect.String: func() error {
			return unmarshalNumOrStr(rows, t)
		},
		reflect.Slice: func() error {
			return unmarshalSlice(rows, t)
		},
	}
	if err = unmarshalMap[kind](); err != nil {
		return
	}
	return
}

func Insert[T any](ctx context.Context, db *sql.DB, dest []T) (newDest []T, err error) {
	t := new(T)
	typeOf := reflect.TypeOf(t).Elem()
	if typeOf.Kind() == reflect.Pointer {
		err = ErrInsertAllow
		return
	}
	tableName := getTableName(t)
	var fields string
	var values string
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	for _, row := range dest {
		kv := getKeysValues(row)
		fields = kv.Key
		values = fmt.Sprintf(`(%s)`, kv.Value)
		sqlStr := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s RETURNING id`, tableName, fields, values)
		outputSql(sqlStr, nil)
		stmt, err := tx.Prepare(sqlStr)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		var lastId int64
		if err = stmt.QueryRowContext(ctx).Scan(&lastId); err != nil {
			return nil, err
		}
		//only for mysql
		//lastId, err := result.LastInsertId()
		//if err != nil {
		//	return nil, err
		//}
		savePrimaryKey(&row, lastId)
		newDest = append(newDest, row)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return
}

func Update[T any](ctx context.Context, db *sql.DB, dest []T, where string, args ...any) error {
	t := new(T)
	typeOf := reflect.TypeOf(t).Elem()
	if typeOf.Kind() == reflect.Pointer {
		return ErrUpdateAllow
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, row := range dest {
		rowSql := generateUpdate(where, row)
		stmt, err := tx.Prepare(rowSql)
		if err != nil {
			tx.Rollback()
			return err
		}
		outputSql(rowSql, args)
		_, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func Delete[T any](ctx context.Context, db *sql.DB, where string, args ...any) error {
	t := new(T)
	typeOf := reflect.TypeOf(t).Elem()
	if typeOf.Kind() == reflect.Pointer {
		return ErrInsertAllow
	}
	where = generateDelete(where, t)
	where, args = parseSqlIn(where, args)
	defer outputSql(where, args)
	stmt, err := db.PrepareContext(ctx, where)
	if err != nil {
		return err
	}
	_, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}
	return nil
}

func unmarshalStruct(rows *sql.Rows, dest any) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	var values []any
	var fieldNames []string
	var fieldsMap = make(map[string]int)
	typeOf := reflect.TypeOf(dest).Elem()
	valueOf := reflect.ValueOf(dest).Elem()
	for curField := 0; curField < valueOf.NumField(); curField++ {
		fName := typeOf.Field(curField).Name
		tag := typeOf.Field(curField).Tag.Get("json")
		if tag == "" {
			tag = toSnake(fName)
		}
		fieldsMap[tag] = curField
	}
	for _, column := range columns {
		if curField, ok := fieldsMap[column]; ok {
			field := valueOf.Field(curField)
			fName := typeOf.Field(curField).Name
			fieldNames = append(fieldNames, fName)
			values = append(values, reflect.New(field.Type()).Interface())
		}
	}
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return err
		}
	}
	for i, column := range fieldNames {
		v := reflect.ValueOf(values[i]).Elem()
		reflect.ValueOf(dest).Elem().FieldByName(column).Set(v)
	}
	return nil
}

func unmarshalSlice(rows *sql.Rows, dest any) error {
	var values []any
	var fieldNames []string
	var fieldsMap = make(map[string]int)
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	destType := reflect.Indirect(reflect.ValueOf(dest).Elem()).Type()
	valueElem := reflect.New(destType.Elem())
	meta := valueElem.Interface()
	typeOf := reflect.TypeOf(meta).Elem()
	valueOf := reflect.ValueOf(meta).Elem()
	for curField := 0; curField < valueOf.NumField(); curField++ {
		fName := typeOf.Field(curField).Name
		tag := typeOf.Field(curField).Tag.Get("json")
		if tag == "" {
			tag = toSnake(fName)
		}
		fieldsMap[tag] = curField
	}
	for _, column := range columns {
		if curField, ok := fieldsMap[column]; ok {
			field := valueOf.Field(curField)
			fName := typeOf.Field(curField).Name
			fieldNames = append(fieldNames, fName)
			values = append(values, reflect.New(field.Type()).Interface())
		}
	}
	var out reflect.Value
	for rows.Next() {
		scanRowValues := values
		err = rows.Scan(scanRowValues...)
		if err != nil {
			return err
		}
		newMeta := meta
		for i, column := range fieldNames {
			v := reflect.ValueOf(values[i]).Elem()
			reflect.ValueOf(newMeta).Elem().FieldByName(column).Set(v)
		}
		out = reflect.Append(reflect.ValueOf(dest).Elem(), reflect.ValueOf(newMeta).Elem())
		reflect.ValueOf(dest).Elem().Set(out)
	}
	return nil
}

func unmarshalNumOrStr(rows *sql.Rows, dest any) error {
	for {
		rows.Next()
		return rows.Scan(dest)
	}
	return nil
}

func getTableName(dest any) string {
	var tableName string
	valueOf := reflect.ValueOf(dest)
	typeOf := reflect.TypeOf(dest)
	if typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
		valueOf = valueOf.Elem()
	}
	if cb := valueOf.MethodByName("TableName"); cb.Kind() == reflect.Func {
		retList := cb.Call([]reflect.Value{})
		if retList != nil {
			tableName = retList[0].String()
		}
	}
	if tableName == `` {
		tableName = toSnake(typeOf.Name())
	}
	return tableName
}

type KV struct {
	Key   string
	Value string
}

func getKeysValues(dest any) *KV {
	typeOf := reflect.TypeOf(dest)
	valueOf := reflect.ValueOf(dest)
	if typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
		valueOf = valueOf.Elem()
	}
	var keys, values []string
	for cur := 0; cur < typeOf.NumField(); cur++ {
		var name string
		if js := typeOf.Field(cur).Tag.Get("json"); js != "" {
			name = js
		} else {
			name = toSnake(typeOf.Field(cur).Name)
		}
		if name == "id" || typeOf.Field(cur).Tag.Get("pri") != "" {
			continue
		}
		value := valueOf.Field(cur).Interface()
		var strValue = fmt.Sprintf("%v", value)
		valueKind := reflect.TypeOf(value).Kind()
		if valueKind == reflect.String || valueKind == reflect.Interface {
			strValue = fmt.Sprintf("'%v'", value)
		}
		if valueKind == reflect.Struct {
			if t, ok := value.(time.Time); ok {
				if t.IsZero() {
					strValue = "DEFAULT"
				} else {
					strValue = t.Format(`'2006-01-02 15:04:05'`)
				}
			}
		}
		if valueKind == reflect.Slice {
			sliceValue, _ := json.Marshal(value)
			strValue = fmt.Sprintf("'%s'", string(sliceValue))
		}
		if valueKind == reflect.Pointer {
			continue
		}
		keys = append(keys, fmt.Sprintf("%s", name))
		values = append(values, strValue)
	}
	return &KV{
		Key:   strings.Join(keys, ","),
		Value: strings.Join(values, ","),
	}
}

var convertSlice2StringFuncMap = map[reflect.Kind]func(meta any) string{
	reflect.String: func(meta any) string {
		if v := meta.([]string); v != nil {
			return `'` + strings.Join(v, "','") + `'`
		}
		return ""
	},
	reflect.Int64: func(meta any) string {
		if list := meta.([]int64); list != nil {
			var l []string
			for _, s := range list {
				l = append(l, fmt.Sprintf("%v", s))
			}
			return strings.Join(l, ",")
		}
		return ""
	},
	reflect.Int: func(meta any) string {
		if list := meta.([]int); list != nil {
			var l []string
			for _, s := range list {
				l = append(l, fmt.Sprintf("%v", s))
			}
			return strings.Join(l, ",")
		}
		return ""
	},
	reflect.Float64: func(meta any) string {
		if list := meta.([]float64); list != nil {
			var l []string
			for _, s := range list {
				l = append(l, fmt.Sprintf("%v", s))
			}
			return strings.Join(l, ",")
		}
		return ""
	},
}

func parseSqlIn(sqlStr string, args []any) (newSqlStr string, newArgs []any) {
	var sliceArgs []string
	for _, arg := range args {
		if reflect.TypeOf(arg).Kind() == reflect.Slice {
			if argValue := reflect.ValueOf(arg); argValue.Len() > 0 {
				elemType := argValue.Index(0).Kind()
				if find, ok := convertSlice2StringFuncMap[elemType]; ok {
					sliceArgs = append(sliceArgs, find(argValue.Interface()))
				}
			}
			continue
		}
		newArgs = append(newArgs, arg)
	}
	rep, _ := regexp.Compile(" (IN|in|In|iN) \\$[0-9]*")
	sliceIdx := 0
	sliceArgLen := len(sliceArgs)
	newSqlStr = rep.ReplaceAllStringFunc(sqlStr, func(s string) string {
		arg := ``
		if sliceIdx < sliceArgLen {
			arg = sliceArgs[sliceIdx]
		}
		sliceIdx++
		if arg == `` {
			return ` IN `
		}
		return ` IN (` + arg + `)`
	})
	return
}

func generateDelete(sqlStr string, dest any) (newSqlStr string) {
	parse := regexp.MustCompile(`(?i)DELETE FROM (.*?) `)
	parseArr := parse.FindAllStringSubmatch(sqlStr, -1)
	if parseArr != nil {
		return sqlStr
	}
	tableName := getTableName(dest)
	return fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, sqlStr)
}
func generateUpdate(sqlStr string, dest any) (newSqlStr string) {
	parse := regexp.MustCompile(`(?i)DELETE (.*?) `)
	parseArr := parse.FindAllStringSubmatch(sqlStr, -1)
	if parseArr != nil {
		return sqlStr
	}
	tableName := getTableName(dest)
	valueOf := reflect.ValueOf(dest)
	typeOf := reflect.TypeOf(dest)
	var sets []string
	for curField := 0; curField < typeOf.NumField(); curField++ {
		fieldName := toSnake(typeOf.Field(curField).Name)
		jsonName := typeOf.Field(curField).Tag.Get("json")
		if jsonName != "" {
			fieldName = jsonName
		}
		isPrimary := fieldName == "id" || typeOf.Field(curField).Tag.Get("pri") != ""
		value := valueOf.Field(curField)
		if isPrimary {
			if sqlStr == "" {
				sqlStr = fmt.Sprintf(`%s = %v`, fieldName, value)
			}
			continue
		}
		var valueStr string
		if value.Kind() == reflect.String || value.Kind() == reflect.Struct || value.Kind() == reflect.Interface {
			valueStr = fmt.Sprintf("'%v'", value)
		} else {
			valueStr = fmt.Sprintf("%v", value)
		}
		if value.Kind() == reflect.Struct {
			if t, ok := value.Interface().(time.Time); ok {
				valueStr = t.Format(`'2006-01-02 15:04:05'`)
			}
		}
		if value.Kind() == reflect.Slice {
			sliceValue, _ := json.Marshal(value.Interface())
			valueStr = fmt.Sprintf("'%s'", string(sliceValue))
		}
		if value.Kind() == reflect.Pointer {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=%s", fieldName, valueStr))
	}
	newSqlStr = fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(sets, ","), sqlStr)
	return
}

func outputSql(s string, args []any) {
	for i, arg := range args {
		v := fmt.Sprintf("%v", arg)
		if reflect.TypeOf(arg).Kind() == reflect.String || reflect.TypeOf(arg).Kind() == reflect.Struct {
			v = fmt.Sprintf("'%v'", arg)
		}
		s = strings.Replace(s, fmt.Sprintf("$%v", i+1), v, i+1)
	}
	log.Printf("[ORM INFO]\t %s \n", s)
}

var savePriFieldMap = map[reflect.Kind]func(value reflect.Value, filedIdx int, lastId int64){
	reflect.Int: func(value reflect.Value, filedIdx int, lastId int64) {
		value.Elem().Field(filedIdx).Set(reflect.ValueOf(int(lastId)))
	},
	reflect.Int64: func(value reflect.Value, filedIdx int, lastId int64) {
		value.Elem().Field(filedIdx).Set(reflect.ValueOf(lastId))
	},
}

func savePrimaryKey(dest any, lastId int64) {
	typeOf := reflect.TypeOf(dest)
	if typeOf.Kind() != reflect.Pointer {
		return
	}
	typeOf = typeOf.Elem()
	valueOf := reflect.ValueOf(dest).Elem()
	if typeOf.Kind() != reflect.Struct {
		return
	}
	for cur := 0; cur < typeOf.NumField(); cur++ {
		name := toSnake(typeOf.Field(cur).Name)
		nameTag := typeOf.Field(cur).Tag.Get("json")
		isPri := typeOf.Field(cur).Tag.Get("pri") != ""
		if name == "id" || nameTag == "id" || isPri {
			fieldKind := valueOf.Field(cur).Kind()
			convert, ok := savePriFieldMap[fieldKind]
			if ok {
				convert(reflect.ValueOf(dest), cur, lastId)
				return
			}
		}
	}
}

func toSnake(name string) string {
	var convert []byte
	for i, asc := range name {
		if asc >= 65 && asc <= 90 {
			asc += 32
			if i > 0 {
				convert = append(convert, 95)
			}
		}
		convert = append(convert, uint8(asc))
	}
	return string(convert)
}
