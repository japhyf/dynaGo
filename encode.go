// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"errors"
	"reflect"
	"runtime"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type TableExistsError struct {
	TableName string
}

func (e TableExistsError) Error() string {
	return "dynaGo: Table named " + e.TableName + " already exists."
}

type UnsupportedKindError struct {
	Kind reflect.Kind
}

func (e UnsupportedKindError) Error() string {
	return "dynaGo: unsuppoted kind: " + e.Kind.String()
}

type MissingKeyError struct {
	Type    reflect.Type
	KeyType string
}

func (e MissingKeyError) Error() string {
	return "dynaGo: Type missing " + e.KeyType + " key: " + e.Type.String()
}

type KeyTypeNotFoundError struct {
	Type reflect.Type
}

func (e *KeyTypeNotFoundError) Error() string {
	return "dynaGo: field " + e.Type.Name() + "has no key type."
}

type OnlyStructsSupportedError struct {
	Kind reflect.Kind
}

func (e *OnlyStructsSupportedError) Error() string {
	return "dynaGo: only structs are supported, not " + e.Kind.String() + "s"
}

type TableKeyCannotBeTypeError struct {
	Type reflect.Type
}

func (e *TableKeyCannotBeTypeError) Error() string {
	return "dynaGo: Table Key values cannot be created from " + e.Type.String()
}

type FieldNameCannotBeError struct {
	FieldName string
}

func (e *FieldNameCannotBeError) Error() string {
	return "dynaGo: FieldName cannot be dynamoDB Key type " + e.FieldName
}

type InvalidEncoderStateType struct {
	State encoderState
}

func (e *InvalidEncoderStateType) Error() string {
	return "dynaGo: Unknown EncoderState type: " + reflect.TypeOf(e.State).Name()
}

type KeyValueOfIncorrectType struct {
	expect reflect.Kind
	found  reflect.Kind
}

func (e *KeyValueOfIncorrectType) Error() string {
	return "dynaGo: Expected key type: " + e.expect.String() + " found:" + e.found.String()
}

type UnsupportedKeyKindError struct {
	Kind reflect.Kind
}

func (e *UnsupportedKeyKindError) Error() string {
	return "dynaGo: partitionkey has unsupported kind - " + e.Kind.String()
}

// Marshal returns a dynamodb.PutItemInput representitive of i
// Any struct to be interpreted by this method must provide a
// Partition Key, marked by the field tag: "HASH", and may
// optionally select a Sort Key using the field tag "RANGE"
// Field tags are modeled after the encoding/json package as
// follows:  A field may have a different name as a dynamoDB
// attribute.  This name can be specified with the field tag
//   `dynaGo:"[alt-name]"`
// Any options in the field tag (such as HASH, or RANGE) must
// be specified after a comma. If the attribute name remains
// the same, then the tage must begin with a leading comma to
// indicate the presence of options:
//   `dynaGo:",HASH"`
//   `dynaGo:"[alt-name],HASH"
// for more examples see pkg/encoding/json.
//
// Table names will simply be composed of the struct name plus
// the letter s.  For instance if there is a
//   type Packet struct {...}
// the associatedd dynamoDB table will be named "Packets" (for now?)
//
// Immsdiately this method only recognizes struct types that are
// composed of exculsively int, string, and structs or slices and
// pointers to any of those types. Any further unexpected type
// will trigger a panic. Additional types shoould be trivial to add
// following the given pattern.
func Marshal(i interface{}) *dynamodb.PutItemInput {
	e := &valueEncoderState{make(map[string]*dynamodb.AttributeValue)}
	encode(e, i)
	tn := TableName(reflect.TypeOf(i))
	return &dynamodb.PutItemInput{Item: e.item, TableName: &tn}
}

func TableName(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name() + "s"
}

// Try to create a table if it doesn't already exist
// If it does exist or cannot be created, return error
//   - Tables are created from structs only, and will panic on any other type
//   - Table name will be [structName] + s (ie type Doc struct {...} => table "Docs")
func CreateTable(svc *dynamodb.DynamoDB, v interface{}, w int64, r int64) error {
	tn := TableName(reflect.TypeOf(v))
	if err := tableExists(svc, tn); err != nil {
		return err
	}
	e := &tableEncoderState{
		keySchema:            make([]*dynamodb.KeySchemaElement, 0),
		attributeDefinitions: make([]*dynamodb.AttributeDefinition, 0),
	}
	encode(e, v)
	params := &dynamodb.CreateTableInput{
		TableName:            &tn,
		KeySchema:            e.keySchema,
		AttributeDefinitions: e.attributeDefinitions,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &r,
			WriteCapacityUnits: &w,
		},
	}
	if _, err := svc.CreateTable(params); err != nil {
		return err
	}
	return nil
}

type encoderState interface{}
type fieldTransform func(fs reflect.StructField, v reflect.Value) bool

// Concerned with encoding structs to 2 types:
// dynamoDB Tables, and dynamoDB Values by way of
// tableEncoderState and valueEncoderState respectively
func encode(e encoderState, i interface{}) {
	foundPKey := false
	v := reflect.ValueOf(i)
	t := v.Type()
	et := reflect.TypeOf(e)

	//allow one possible level of indirection
	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			panic(errors.New("Cannot encode nil ptr."))
		}
		t, v = t.Elem(), v.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic(&OnlyStructsSupportedError{t.Kind()})
	}
	var ftr fieldTransform
	switch es := e.(type) {
	case *tableEncoderState:
		ftr = func(fs reflect.StructField, fv reflect.Value) bool {
			str := tableEncoder(fs.Type)(es, fs, fv)
			return str == dynamodb.KeyTypeHash
		}
	case *valueEncoderState:
		ftr = func(fs reflect.StructField, fv reflect.Value) bool {
			fn := getAttrName(fs)
			valueEncoder(fs.Type)(es, fn, fv)
			return true
		}
	default:
		panic(&InvalidEncoderStateType{et})
	}
	for n := 0; n < t.NumField(); n++ {
		fs, fv := t.Field(n), v.Field(n)
		// expect to find a primary key
		foundPKey = ftr(fs, fv) || foundPKey
	}
	if !foundPKey {
		panic(&MissingKeyError{t, dynamodb.KeyTypeHash})
	}
}

//-- UTIL --//
// could be cached
func tableExists(svc *dynamodb.DynamoDB, tn string) error {
	params := &dynamodb.ListTablesInput{}
	resp, err := svc.ListTables(params)
	if err != nil {
		return err
	}
	for _, name := range resp.TableNames {
		if *name == tn {
			return TableExistsError{tn}
		}
	}
	return nil
}

// The dynamoDB attribute name is determined by:
// if the field tags contains a name use that name
// if not, just use the native GoLang field name
// THIS METHOD PANICS IF the tags name the field
// "HASH", or "RANGE" as this is assumed to be a
// mistake (missing leading comma in field tag)
func getAttrName(s reflect.StructField) string {
	fn, _ := parseTag(s.Tag.Get("dynaGo"))
	if fn == dynamodb.KeyTypeHash || fn == dynamodb.KeyTypeRange {
		panic(&FieldNameCannotBeError{fn})
	}
	if fn == "" {
		fn = s.Name
	}
	return fn
}

// Determine if this field is a dynamoDB key
// if it is return the type from the below set
//   - dynamodb.KeyTypeHash
//   - dynamoDB.KeyTypeRange
// if it is not, return "" and an error
func getKeyType(s reflect.StructField, v reflect.Value) (string, error) {
	_, o := parseTag(s.Tag.Get("dynaGo"))
	if o.Contains(dynamodb.KeyTypeHash) {
		return dynamodb.KeyTypeHash, nil
	}
	if o.Contains(dynamodb.KeyTypeRange) {
		return dynamodb.KeyTypeRange, nil
	}
	return "", &KeyTypeNotFoundError{v.Type()}
}

// depth-first pursuit of a partition key through structs marked HASH
// if a string is not found at a leaf, this method will panic.
func getPartitionKey(t reflect.Type) []int {
	return getKeyAttributePath(t, dynamodb.KeyTypeHash)
}

// depth-first pursuit of a range key through structs marked RANGE
// in the origin struct, and HASH thereafter (as depth increases
// beyond 0).if a string is not found at a leaf, returns MissingKeyError
func getRangeKey(t reflect.Type) (i []int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			if s, ok := r.(string); ok {
				panic(s)
			}
			err = r.(error)
		}
	}()
	i, err = getKeyAttributePath(t, dynamodb.KeyTypeRange), nil
	return
}

// recursive, panics when fails
// the RANGE KeyType (kt) - is only relevant for struct depth 0
// ie. if the RANGE key type is a struct, this method returns the
//     HASH Key of the child type for the RANGE
func getKeyAttributePath(t reflect.Type, kt string) []int {
	for n := 0; n < t.NumField(); n++ {
		f := t.Field(n)
		_, opts := parseTag(f.Tag.Get("dynaGo"))
		if !opts.Contains(kt) {
			continue
		}
		switch f.Type.Kind() {
		//int?  not yet.
		case reflect.Ptr:
			return append([]int{n}, getKeyAttributePath(f.Type.Elem(), dynamodb.KeyTypeHash)...)
		case reflect.String:
			return []int{n}
		case reflect.Struct:
			return append([]int{n}, getKeyAttributePath(f.Type, dynamodb.KeyTypeHash)...)
		}
	}
	panic(&MissingKeyError{t, kt})
}

type Key struct {
	tbln string
	attr map[string]*dynamodb.AttributeValue
}

// To put items to dynamoDB is one thing (Marshal), but to get items from
// dynamoDB often requires a GetItemInput (if the item is fetched by primary key directly)
// this method will convert a struct i with a key value ...k [partition key, rangekey]
// to a GetItemInput as long as the struct is properly tagged, and the
// partition key and range key are of the type descibed by the struct
//
// This method may have some logical overlap with encode()
// should look into that someday.  May just be able to grab the KeySchema?
func CreateKey(t reflect.Type, vs ...interface{}) (Key, error) {
	//sanity check
	if len(vs) < 1 {
		return Key{}, errors.New("dynaGo: no partition key provided for GetItemInput")
	}
	k := Key{
		tbln: TableName(t),
		attr: make(map[string]*dynamodb.AttributeValue),
	}

	//partition key, panics if not found
	pki := getPartitionKey(t)
	pkn, pka, err := getKeynameAndAttribute(t, pki, vs[0])
	if err != nil {
		return Key{}, err
	}
	k.attr[pkn] = pka

	//range key may not exist
	rki, err := getRangeKey(t)
	if err == nil {
		if len(vs) < 2 {
			return Key{}, errors.New("dynaGo: range key found, no value provided for GetItemInput")
		}
		rkn, rka, err := getKeynameAndAttribute(t, rki, vs[1])
		if err != nil {
			return Key{}, err
		}
		k.attr[rkn] = rka
	}

	return k, nil

}

func (k *Key) GetItemInput() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		TableName: &k.tbln,
		Key:       k.attr,
	}
}

func (k *Key) AppendToBatchGet(b *dynamodb.BatchGetItemInput) {
	if b.RequestItems == nil {
		b.RequestItems = make(map[string]*dynamodb.KeysAndAttributes)
	}
	if _, ok := b.RequestItems[k.tbln]; !ok {
		b.RequestItems[k.tbln] = &dynamodb.KeysAndAttributes{}
	}
	b.RequestItems[k.tbln].Keys = append(b.RequestItems[k.tbln].Keys, k.attr)
}

func getKeynameAndAttribute(t reflect.Type, i []int, k interface{}) (kn string, ka *dynamodb.AttributeValue, err error) {
	//value from leaf
	sf := t.FieldByIndex(i)
	ka, err = createAttribute(sf, k)
	if err != nil {
		return "", nil, err
	}
	//name from root
	rootkf := t.Field(i[0])
	kn = getAttrName(rootkf)
	return
}

// checks to make sure the key value given matches the type
// expected, and then returns a *dyanmodb.AttributeValue that
// describes the field / value pair.
func createAttribute(sf reflect.StructField, k interface{}) (ka *dynamodb.AttributeValue, err error) {
	switch sf.Type.Kind() {
	case reflect.String:
		s, ok := k.(string)
		if !ok {
			err = &KeyValueOfIncorrectType{reflect.String, reflect.TypeOf(k).Kind()}
			return
		}
		ka = &dynamodb.AttributeValue{S: &s}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.ValueOf(k)
		if !isInt(v) {
			err = &KeyValueOfIncorrectType{reflect.Int, v.Kind()}
			return
		}
		s := strconv.FormatInt(v.Int(), 10)
		ka = &dynamodb.AttributeValue{N: &s}
	default:
		panic(&UnsupportedKeyKindError{sf.Type.Kind()})
	}
	return
}

// check if value is an int.. helper for AsGetItemInput
func isInt(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	}
	return false
}
