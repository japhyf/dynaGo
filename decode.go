// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type UnsupportedTypeDecoderError struct {
	Type reflect.Type
}

func (e UnsupportedTypeDecoderError) Error() string {
	return "dynaGo: decoding " + e.Type.String() + " unimplemented"
}

type InvalidDecodeError struct {
	Type reflect.Type
}

func (e *InvalidDecodeError) Error() string {
	if e.Type == nil {
		return "dynaGo: decode(nil)"
	}
	return "dynaGo: decode(nil " + e.Type.String() + ")"
}

type UnsupportedArrayElementType struct {
	Type reflect.Type
}

func (e UnsupportedArrayElementType) Error() string {
	return "dynaGo: decoding " + e.Type.String() + " unimplemented"
}

// Decode pulls structs (of type i interface{}) from
// map[string]*dynamodb.AttributeValue, where  string is the
// fieldname (or overriden by the dynaGo: fieldtag) and the
// atributeValue is the value to be stored in the field.
func Decode(m map[string]*dynamodb.AttributeValue, i interface{}) error {
	rv := reflect.ValueOf(i)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidDecodeError{rv.Type()}
	}
	ev := rv.Elem()
	et := ev.Type()
	if ev.Kind() != reflect.Struct {
		return &OnlyStructsSupportedError{ev.Kind()}
	}
	for i, field := range typeFields(et) {
		if av, ok := m[field.name]; ok {
			f := ev.Field(i)
			decoder(f.Type())(av, f)
		}
	}
	return nil
}

func decoder(t reflect.Type) decoderFunc {
	switch t.Kind() {
	case reflect.String:
		return stringDecoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intDecoder
	case reflect.Ptr:
		return newPtrDecoder(t)
	case reflect.Struct:
		return structDecoder
	case reflect.Slice, reflect.Array:
		return newSliceDecoder(t)
	default:
		return UnsupportedTypeDecoder
	}

}

// --DECODERS-- //
type decoderFunc func(av *dynamodb.AttributeValue, rv reflect.Value)

func UnsupportedTypeDecoder(av *dynamodb.AttributeValue, rv reflect.Value) {
	panic(UnsupportedTypeDecoderError{rv.Type()})
}
func stringDecoder(av *dynamodb.AttributeValue, rv reflect.Value) {
	rv.SetString(*av.S)
}
func intDecoder(av *dynamodb.AttributeValue, rv reflect.Value) {
	n, _ := strconv.ParseInt(*av.N, 10, 64)
	rv.SetInt(n)
}

type sliceDecoder struct {
	explode     exploder
	elemDecoder decoderFunc
}

func (sd *sliceDecoder) decode(av *dynamodb.AttributeValue, rv reflect.Value) {
	avs := sd.explode(av)
	l := len(avs)
	rv.Set(reflect.MakeSlice(rv.Type(), l, l))
	for i, a := range avs {
		sd.elemDecoder(a, rv.Index(i))
	}
}

// Creates a new slice decoder.
// There are several aspects of the approach that constrain the solution
//     - Arrays stored in this way are actually SETs (members will not repeat)
//     - the *dynamodb.AttributeValue returned will be a SET of the undelying
//       values of the array and has to be 'exploded' to reuse decode()
//     - the type stored within the array should be consumable by decode()
//
// The reuse of decode() keeps the code more consise, but it may be
// simpler/faster to just re-implement a string/int decoder for arrays
//
// As it stands, this method should succesfully consume strings, ints, ptrs
// and structs, or any possible composition of those elements.
// IT WILL NOT CONSUME ARRAYS OF ARRAYS, and strictly speaking, this wouldn't
// be partifularly useful in a DB - the data wouldn't be accessible / normalized.
func newSliceDecoder(t reflect.Type) decoderFunc {
	et := t.Elem()
	dec := sliceDecoder{newExploder(et), decoder(et)}
	return dec.decode
}

type exploder func(av *dynamodb.AttributeValue) []*dynamodb.AttributeValue

func newExploder(t reflect.Type) exploder {
	switch t.Kind() {
	case reflect.String:
		return func(av *dynamodb.AttributeValue) []*dynamodb.AttributeValue {
			l := len(av.SS)
			arr := make([]*dynamodb.AttributeValue, 0, l)
			for _, s := range av.SS {
				arr = append(arr, &dynamodb.AttributeValue{S: s})
			}
			return arr
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(av *dynamodb.AttributeValue) []*dynamodb.AttributeValue {
			l := len(av.NS)
			arr := make([]*dynamodb.AttributeValue, 0, l)
			for _, s := range av.SS {
				arr = append(arr, &dynamodb.AttributeValue{N: s})
			}
			return arr
		}
	case reflect.Struct:
		i := getPartitionKey(t)
		return newExploder(t.FieldByIndex(i).Type)
	case reflect.Ptr:
		return newExploder(t.Elem())
	default:
		panic(UnsupportedArrayElementType{t})
	}
}

//if a struct is found, it's almost certainly the result of a pointer
//dynaGo only Stores one layer of values, so we have to find the Hash key field,
//compose the hierarchy above the field, and set that with the attribute value.
func structDecoder(av *dynamodb.AttributeValue, rv reflect.Value) {
	i := getPartitionKey(rv.Type())
	structCompose(rv, i)
	fv := rv.FieldByIndex(i)
	decoder(fv.Type())(av, fv)
}

// this function takes a value, and a field index and instantiates any
// nil pointers it finds in the tree between the root and the leaf.
// for fun and games checkout https://play.golang.org/p/iI4Ix00Fyc
func structCompose(rv reflect.Value, i []int) {
	if len(i) > 0 {
		f := rv.Field(i[0])
		switch f.Type().Kind() {
		case reflect.Struct:
			structCompose(f, i[1:])
		case reflect.Ptr:
			if f.IsNil() {
				f.Set(reflect.New(f.Type().Elem()))
			}
			structCompose(f.Elem(), i[1:])
		}
	}
}

//stores the underlying Elem decoder
type ptrDecoder struct {
	elemDecoder decoderFunc
}

func (pd *ptrDecoder) decode(av *dynamodb.AttributeValue, rv reflect.Value) {
	if rv.IsNil() {
		rv.Set(reflect.New(rv.Type().Elem()))
	}
	pd.elemDecoder(av, rv.Elem())
}
func newPtrDecoder(t reflect.Type) decoderFunc {
	dec := &ptrDecoder{decoder(t.Elem())}
	return dec.decode
}

// --UTIL-- //

// The name stored in this struct helps map from the
// DB attributeName (or column) to the struct field name.
// The values cached here to avoid noisey functions
type field struct {
	name string

	index []int
	typ   reflect.Type
}

func newField(sf reflect.StructField) field {
	return field{
		name:  getAttrName(sf),
		index: sf.Index,
		typ:   sf.Type,
	}
}

func typeFields(t reflect.Type) (fields []field) {
	fields = make([]field, 0)

	for i := 0; i < t.NumField(); i++ {
		sf := newField(t.Field(i))
		fields = append(fields, sf)
	}
	return
}
