// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// This part of the package is somewhat ad-hoc and disorganized.
// It needs further refinment that I'm not willing to invest in at
// the moment.

type key struct {
	pkn  string
	rkn  string
	tbln string
	attr map[string]*dynamodb.AttributeValue
}

type KeyMaker func(...interface{}) (key, error)

// To put items to dynamoDB is one thing (Marshal), but to get items from
// dynamoDB often requires a GetItemInput (if the item is fetched by primary key directly)
// this method will convert a struct i with a key value ...k [partition key, rangekey]
// to a GetItemInput as long as the struct is properly tagged, and the
// partition key and range key are of the type descibed by the struct
//
// This method may have some logical overlap with encode()
// should look into that someday.  May just be able to grab the KeySchema?
func CreateKeyMaker(t reflect.Type) KeyMaker {
	priK := key{
		tbln: TableName(t),
	}
	//partition key, panics if not found
	pki := getPartitionKey(t)
	pF := func(kv interface{}) (string, dynamodb.AttributeValue, error) {
		return getKeynameAndAttribute(t, pki, kv)
	}

	//range key may not exist
	rki, err := getRangeKey(t)
	if err != nil {
		// no range key
		return func(ks ...interface{}) (key, error) {
			if len(ks) < 1 {
				es := fmt.Sprintf("dynaGo:%s KeyMaker: incorrect num args [%d]", t.Name(), len(ks))
				return key{}, errors.New(es)
			}
			k, v, err := pF(ks[0])
			if err != nil {
				return key{}, err
			}
			priK.pkn = k
			priK.attr = make(map[string]*dynamodb.AttributeValue)
			priK.attr[k] = &v

			return priK, nil
		}
	}
	rF := func(rk interface{}) (string, dynamodb.AttributeValue, error) {
		return getKeynameAndAttribute(t, rki, rk)
	}

	return func(ks ...interface{}) (key, error) {
		if len(ks) < 2 {
			es := fmt.Sprintf("dynaGo:%s KeyMaker: incorrect num args [%d]", t.Name(), len(ks))
			return key{}, errors.New(es)
		}
		pk, pv, err := pF(ks[0])
		if err != nil {
			return key{}, err
		}
		priK.pkn = pk
		priK.attr = make(map[string]*dynamodb.AttributeValue)
		priK.attr[pk] = &pv

		rk, rv, err := rF(ks[1])
		if err != nil {
			return key{}, err
		}
		priK.rkn = rk
		priK.attr[rk] = &rv
		return priK, nil
	}
}

func GetItemInput(km KeyMaker, kv ...interface{}) (*dynamodb.GetItemInput, error) {
	k, err := km(kv...)
	if err != nil {
		return nil, err
	}
	return &dynamodb.GetItemInput{
		TableName: &k.tbln,
		Key:       k.attr,
	}, nil
}

func AppendToBatchGet(b *dynamodb.BatchGetItemInput, km KeyMaker, kv ...interface{}) error {
	k, err := km(kv...)
	if err != nil {
		return err
	}
	if b.RequestItems == nil {
		b.RequestItems = make(map[string]*dynamodb.KeysAndAttributes)
	}
	if _, ok := b.RequestItems[k.tbln]; !ok {
		b.RequestItems[k.tbln] = &dynamodb.KeysAndAttributes{}
	}
	b.RequestItems[k.tbln].Keys = append(b.RequestItems[k.tbln].Keys, k.attr)
	return err
}
func QueryOnPartition(km KeyMaker, kv interface{}) (*dynamodb.QueryInput, error) {
	kce := "#name = :value"
	k, err := km(kv, "")
	if err != nil {
		return nil, err
	}
	return &dynamodb.QueryInput{
		TableName: &k.tbln,
		ExpressionAttributeNames: map[string]*string{
			"#name": &k.pkn,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":value": k.attr[k.pkn],
		},
		KeyConditionExpression: &kce,
	}, nil
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
		case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return []int{n}
		case reflect.Ptr:
			return append([]int{n}, getKeyAttributePath(f.Type.Elem(), dynamodb.KeyTypeHash)...)
		case reflect.Struct:
			return append([]int{n}, getKeyAttributePath(f.Type, dynamodb.KeyTypeHash)...)
		}
	}
	panic(&MissingKeyError{t, kt})
}

func getKeynameAndAttribute(t reflect.Type, i []int, k interface{}) (kn string, ka dynamodb.AttributeValue, err error) {
	//value from leaf
	sf := t.FieldByIndex(i)
	ka, err = createAttribute(sf, k)
	if err != nil {
		return "", dynamodb.AttributeValue{}, err
	}
	//name from root
	rootkf := t.Field(i[0])
	kn = getAttrName(rootkf)
	return
}

// checks to make sure the key value given matches the type
// expected, and then returns a *dyanmodb.AttributeValue that
// describes the field / value pair.
func createAttribute(sf reflect.StructField, k interface{}) (ka dynamodb.AttributeValue, err error) {
	switch sf.Type.Kind() {
	case reflect.String:
		s, ok := k.(string)
		if !ok {
			err = &KeyValueOfIncorrectType{reflect.String, reflect.TypeOf(k).Kind()}
			return
		}
		ka = dynamodb.AttributeValue{S: &s}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.ValueOf(k)
		if !isInt(v) {
			err = &KeyValueOfIncorrectType{reflect.Int, v.Kind()}
			return
		}
		s := strconv.FormatInt(v.Int(), 10)
		ka = dynamodb.AttributeValue{N: &s}
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
