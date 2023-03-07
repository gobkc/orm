package orm

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Encrypt(codeData string, saltKey string) string {
	dataArr := []rune(codeData)
	keyArr := []byte(saltKey)
	keyLen := len(keyArr)

	var tmpList []int

	for index, value := range dataArr {
		base := int(value)
		dataString := base + int(0xFF&keyArr[index%keyLen])
		tmpList = append(tmpList, dataString)
	}

	var str string

	for _, value := range tmpList {
		str += "_" + fmt.Sprintf("%d", value)
	}
	return base64.StdEncoding.EncodeToString([]byte(str))
}

func Decrypt(ntData string, saltKey string) string {
	decodeStr, err := base64.StdEncoding.DecodeString(ntData)
	if err != nil {
		return ""
	}
	ntData = string(decodeStr)
	strLen := len(ntData)
	newData := []rune(ntData)
	resultData := string(newData[1:strLen])
	dataArr := strings.Split(resultData, "_")
	keyArr := []byte(saltKey)
	keyLen := len(keyArr)

	var tmpList []int

	for index, value := range dataArr {
		base, _ := strconv.Atoi(value)
		dataString := base - int(0xFF&keyArr[index%keyLen])
		tmpList = append(tmpList, dataString)
	}

	var str string

	for _, val := range tmpList {
		str += string(rune(val))
	}
	return str
}

func Random(length int) (str string) {
	if length == 0 {
		return
	}
	var (
		randByte  = make([]byte, length)
		formatStr []string
		outPut    []interface{}
		byteHalf  uint8 = 127
	)
	rand.Read(randByte)
	for _, b := range randByte {
		if b > byteHalf {
			formatStr = append(formatStr, "%X")
		} else {
			formatStr = append(formatStr, "%x")
		}
		outPut = append(outPut, b)
	}
	if str = fmt.Sprintf(strings.Join(formatStr, ""), outPut...); len(str) > length {
		str = str[:length]
	}
	return
}

func BindDefault(dest interface{}) error {
	t := reflect.TypeOf(dest)
	if dt := t.Kind(); dt != reflect.Ptr {
		return errors.New("dest must be a struct pointer")
	}
	if dt := t.Elem().Kind(); dt != reflect.Struct {
		return errors.New("dest must be a struct pointer")
	}
	v := reflect.ValueOf(dest).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		tag := field.Tag
		df := tag.Get("default")
		if fmt.Sprintf("%v", v.Field(i).Interface()) != "" {
			continue
		}
		switch fk := field.Type.Kind(); fk {
		case reflect.String:
			v.Field(i).SetString(df)
		case reflect.Int:
			val, err := strconv.Atoi(df)
			if err != nil {
				return err
			}
			v.Field(i).Set(reflect.ValueOf(val))
		case reflect.Int64:
			val, err := strconv.ParseInt(df, 10, 64)
			if err != nil {
				return err
			}
			v.Field(i).SetInt(val)
		case reflect.Int32:
			val, err := strconv.ParseInt(df, 10, 32)
			if err != nil {
				return err
			}
			newV := int32(val)
			v.Field(i).Set(reflect.ValueOf(newV))
		case reflect.Float32:
			val, err := strconv.ParseFloat(df, 32)
			if err != nil {
				return err
			}
			newVal := float32(val)
			v.Field(i).Set(reflect.ValueOf(newVal))
		case reflect.Float64:
			val, err := strconv.ParseFloat(df, 64)
			if err != nil {
				return err
			}
			v.Field(i).SetFloat(val)
		case reflect.Bool:
			var val bool
			if df = strings.ToUpper(df); df == "TRUE" {
				val = true
			}
			v.Field(i).SetBool(val)
		default:
			return errors.New("unsupported type")
		}
	}
	return nil
}

func TrimAll(data any) (err error) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.Ptr:
		switch reflect.ValueOf(data).Elem().Kind() {
		case reflect.String:
			old := data.(*string)
			reflect.ValueOf(data).Elem().SetString(strings.TrimSpace(*old))
			return
		case reflect.Struct:
			for idx := 0; idx < reflect.ValueOf(data).Elem().NumField(); idx++ {
				if fKind := reflect.ValueOf(data).Elem().Field(idx).Kind(); fKind == reflect.String {
					oldStr := reflect.ValueOf(data).Elem().Field(idx).String()
					newStr := strings.TrimSpace(oldStr)
					reflect.ValueOf(data).Elem().Field(idx).SetString(newStr)
				}
			}
			return
		}
	}
	return errors.New(`dest must be a string/struct pointer`)
}

func ConvertJsonb[T []any | any](list T) string {
	jByte, _ := json.Marshal(list)
	js := string(jByte)
	if js == "" {
		if reflect.TypeOf(list).Kind() == reflect.Slice {
			js = "[]"
		} else {
			js = "{}"
		}
	}
	return js
}

func ConvertObject[Object any, D []byte | string](dest D) *Object {
	t := new(Object)
	json.Unmarshal([]byte(dest), t)
	return t
}

func Int[T int | int64 | int32](dest string) T {
	i, _ := strconv.ParseInt(dest, 64, 10)
	return T(i)
}
