package orm

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"regexp"
	"strings"
)

var allows = []reflect.Kind{reflect.Struct, reflect.Slice, reflect.Int, reflect.Int64, reflect.String, reflect.Float64}
var ErrAllow = fmt.Errorf("query: allow list: reflect.Struct/reflect.Slice/reflect.Int/reflect.Int64/reflect.String/reflect.Float64")
var ErrInsertAllow = fmt.Errorf("query: allow list: reflect.Struct")

func Query[T any](ctx context.Context, db *sql.DB, sqlStr string, args ...any) (t *T, err error) {
	t = new(T)
	sqlStr, args = parseSqlIn(sqlStr, args)
	stmt, err := db.PrepareContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
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
		sqlStr := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`, tableName, fields, values)
		stmt, err := tx.Prepare(sqlStr)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		result, err := stmt.ExecContext(ctx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		lastId, err := result.LastInsertId()
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		savePrimaryKey(&row, lastId)
		newDest = append(newDest, row)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return
}
func Delete[T any](ctx context.Context, db *sql.DB, where string, args ...any) error {
	t := new(T)
	typeOf := reflect.TypeOf(t).Elem()
	if typeOf.Kind() == reflect.Pointer {
		return ErrInsertAllow
	}
	where = generateDelete(where, t)
	where, args = parseSqlIn(where, args)

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
	rows.Next()
	err = rows.Scan(values...)
	if err != nil {
		return err
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
	rows.Next()
	return rows.Scan(dest)
}

func getTableName(dest any) string {
	var tableName string
	valueOf := reflect.ValueOf(dest).Elem()
	typeOf := reflect.TypeOf(dest).Elem()
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
		value := valueOf.Field(cur).Interface()
		var strValue = fmt.Sprintf("%v", value)
		if reflect.TypeOf(value).Kind() == reflect.String {
			strValue = fmt.Sprintf("'%v'", value)
		}
		keys = append(keys, fmt.Sprintf("`%s`", name))
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
		if list := meta.([]int64); list != nil {
			var l []string
			for _, s := range list {
				l = append(l, fmt.Sprintf("%v", s))
			}
			return strings.Join(l, ",")
		}
		return ""
	},
	reflect.Float64: func(meta any) string {
		if list := meta.([]int64); list != nil {
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
	rep, _ := regexp.Compile(" (IN|in|In|iN) \\??")
	sliceIdx := 0
	sliceArgLen := len(sliceArgs)
	newSqlStr = rep.ReplaceAllStringFunc(sqlStr, func(s string) string {
		arg := ``
		if sliceIdx < sliceArgLen {
			arg = sliceArgs[sliceIdx]
		}
		sliceIdx++
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
