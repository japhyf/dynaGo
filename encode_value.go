// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type valueEncoderFunc func(e *valueEncoderState, n string, v reflect.Value) string

func valueEncoder(t reflect.Type) valueEncoderFunc {
	switch t.Kind() {
	case reflect.Slice:
		return sliceValueEncoder
	case reflect.Struct:
		return structValueEncoder
	case reflect.String:
		return stringValueEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intValueEncoder
	case reflect.Ptr:
		return newPtrValueEncoder(t)
	default:
		return valueUnsupportedTypeEncoder
	}
}

func valueUnsupportedTypeEncoder(e *valueEncoderState, n string, v reflect.Value) string {
	e.Error(&UnsupportedKindError{v.Type().Kind()})
	return ""
}

type valueEncoderState struct {
	item map[string]*dynamodb.AttributeValue
}

func (e *valueEncoderState) Error(err error) {
	panic(err)
}

func intValueEncoder(e *valueEncoderState, n string, v reflect.Value) string {
	str := strconv.FormatInt(v.Int(), 10)
	if e != nil {
		e.item[n] = &dynamodb.AttributeValue{N: &str}
	}
	return str
}
func stringValueEncoder(e *valueEncoderState, n string, v reflect.Value) string {
	str := v.String()
	if e != nil {
		e.item[n] = &dynamodb.AttributeValue{S: &str}
	}
	return str
}
func structValueEncoder(e *valueEncoderState, n string, v reflect.Value) string {
	i := GetPartitionKey(v.Type())
	str := v.FieldByIndex(i).String()
	if e != nil {
		e.item[n] = &dynamodb.AttributeValue{S: &str}
	}
	return str
}
func sliceValueEncoder(e *valueEncoderState, n string, v reflect.Value) string {
	l, et := v.Len(), v.Type().Elem()
	arrPtr := make([]*string, l)
	arrEle := make([]string, l)
	enc := valueEncoder(et)

	// special case is []byte, which will look like []int8
	if et.Kind() == reflect.Uint8 {
		b := v.Interface().([]byte)
		e.item[n] = &dynamodb.AttributeValue{B: b}
		return "[" + fmt.Sprintf("% x", b) + "]"
	}

	for i := 0; i < l; i++ {
		arrEle[i] = enc(nil, n, v.Index(i))
		arrPtr[i] = &arrEle[i]
	}
	if e != nil {
		switch et.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			e.item[n] = &dynamodb.AttributeValue{NS: arrPtr}
		default:
			e.item[n] = &dynamodb.AttributeValue{SS: arrPtr}
		}
	}
	return "[" + strings.Join(arrEle, ",") + "]"
}

// the pointer will have a single sustained type no matter how
// many times we use this encoder to encode it, so we cache the
// valueEncoderFunc to avoid type lookup everytime we use it
type ptrValueEncoder struct {
	elemEnc valueEncoderFunc
}

func (pe *ptrValueEncoder) encode(e *valueEncoderState, n string, v reflect.Value) string {
	if v.IsNil() {
		return ""
	}
	return pe.elemEnc(e, n, v.Elem())
}

func newPtrValueEncoder(t reflect.Type) valueEncoderFunc {
	enc := &ptrValueEncoder{valueEncoder(t.Elem())}
	return enc.encode
}
