// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type tableEncoderState struct {
	keySchema            []*dynamodb.KeySchemaElement
	attributeDefinitions []*dynamodb.AttributeDefinition
}

func (e *tableEncoderState) Error(err error) {
	panic(err)
}

func tableEncoder(t reflect.Type) tableEncoderFunc {
	switch t.Kind() {
	case reflect.Slice:
		return sliceTableEncoder
	case reflect.Struct:
		return structTableEncoder
	case reflect.String:
		return stringTableEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intTableEncoder
	case reflect.Ptr:
		return newPtrTableEncoder(t)
	default:
		return tableUnsupportedTypeEncoder
	}
}

type tableEncoderFunc func(e *tableEncoderState, s reflect.StructField, v reflect.Value) string

func intTableEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	return attributeEncoder(e, s, v, dynamodb.ScalarAttributeTypeN)
}
func stringTableEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	return attributeEncoder(e, s, v, dynamodb.ScalarAttributeTypeS)
}
func structTableEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	return attributeEncoder(e, s, v, dynamodb.ScalarAttributeTypeS)
}
func sliceTableEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	if _, err := getKeyType(s, v); err == nil {
		e.Error(&TableKeyCannotBeTypeError{v.Type()})
	}
	return ""
}

type ptrTableEncoder struct {
	elemZer reflect.Value
	elemEnc tableEncoderFunc
}

func (pe *ptrTableEncoder) encode(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	//the value passed as v will most likely be empty, so used zero value by default
	return pe.elemEnc(e, s, pe.elemZer)
}

func newPtrTableEncoder(t reflect.Type) tableEncoderFunc {
	et := t.Elem()
	enc := &ptrTableEncoder{reflect.Zero(et), tableEncoder(et)}
	return enc.encode
}

func tableUnsupportedTypeEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value) string {
	e.Error(&UnsupportedKindError{v.Type().Kind()})
	return ""
}

func attributeEncoder(e *tableEncoderState, s reflect.StructField, v reflect.Value, st string) string {
	an := getAttrName(s)
	kt, err := getKeyType(s, v)
	//if this is not a key attribute, the table schema doesn't care
	if err != nil {
		return ""
	}
	e.keySchema = append(e.keySchema,
		&dynamodb.KeySchemaElement{
			AttributeName: &an,
			KeyType:       &kt,
		})
	e.attributeDefinitions = append(e.attributeDefinitions,
		&dynamodb.AttributeDefinition{
			AttributeName: &an,
			AttributeType: &st,
		})
	return kt
}
