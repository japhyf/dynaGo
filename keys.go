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

type keyType int

//types of keys in dynamodb
const (
	primary keyType = iota
	secGbl  keyType = iota
	secLoc  keyType = iota
)

//key is the type used to query a dynamodb.table
type key struct {
	t    keyType
	pkn  string
	rkn  string
	tbln string
	attr map[string]*dynamodb.AttributeValue
}

//KeyMaker is an interface specifying a function that takes as a
//set of arguments any values that are intended to be the values
//of the attributes of the the key specified by this keyMaker,
//and returns a fully populated key with which queries can be made
//against the table specified during its creation - see CreateKeyMaker
type KeyMaker func(...interface{}) (key, error)

//createSecondaryIndex takes a type that describes an existing dynamodb table
//and uses a key to generate a secondary index in dynamodb in the table.
//
// the format for the returned index name will be as follows:
// if there is only a partition key:
//   the name of the index will be [partitionKey]Index
//   for example if the partition key of the index is email in the Usrs table
//   the returned name will be 'emailIndex'
// if there is a partition and a range key:
//   [partitionKey]By[rangeKey]Index
//   for example given the partition key 'routeId' and the range 'Timestamp' in
//   the Messages table, the returned indexname will be 'routeIdByTimestampIndex'
// the key needs to specify the partition key name, the range key name, and can
// use the attr map to specify non-Key-attributes that should be returned by this
// index
//TODO_JAPHY
func createSecondaryIndex(rt reflect.Type, k key) (string, error) {
    //allowing for 1 level of indirection
    //not sure if I can use functions from other files, if so just:
    tn:=TableName(rt)
    if k.rkn == "" {
        in := k.pkn + "Index"
        update :=  &dynamodb.UpdateTableInput{
            AttributeDefinitions: k.attr,
            TableName: tn,
            ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
                ReadCapacityUnits: aws.Int64(10), //# required
                WriteCapacityUnits: aws.Int64(10), //# required
            },
            GlobalSecondaryIndexUpdates: &dynamodb.GlobalSecondaryIndexUpdate{
                    Create: &CreateGlobalSecondaryIndexAction{
                        IndexName: in,
                        KeySchema: []*KeySchemaElement{ //left off here
                            &KeySchemaElement{
                                AttributeName: k.pkn,
                                KeyType: "HASH",
                            },
                        },
                        //not quite sure what this part means
                        Projection: &Projection {
                            ProjectionType: "INCLUDE",
                            NonKeyAttributes: getKeys(k.attr),
                        },
                    },
                },
        }
    }else {
        in := k.pkn + "By" + k.rkn + "Index"
        update :=  &dynamodb.UpdateTableInput{
            AttributeDefinitions: k.attr,
            TableName: tn,
            ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
                ReadCapacityUnits: aws.Int64(10), //# required
                WriteCapacityUnits: aws.Int64(10), //# required
            },
            GlobalSecondaryIndexUpdates: &dynamodb.GlobalSecondaryIndexUpdate{
                    Create: &CreateGlobalSecondaryIndexAction{
                        IndexName: in,
                        KeySchema: []*KeySchemaElement{ //left off here
                            &KeySchemaElement{
                                AttributeName: k.pkn,
                                KeyType: "HASH",
                            },
			    &KeySchemaElement{
                                AttributeName: k.rkn,
                                KeyType: "RANGE",
			    },
                        },
                        //not quite sure what this part means
                        Projection: &Projection {
                            ProjectionType: "INCLUDE",
                            NonKeyAttributes: getKeys(k.attr),
                        },
                    },
                },
	}
    }
    dynamodb.UpdateTable(&update);

	//YOUR CODE GOES HERE

	// Make sure the table exists

	// Make sure the index doesn't already exist

	// insert new index that can be queried by key
	return "", nil
}
func getKeys(attr map[string]*dynamodb.AttributeValue) []*string {
    out := make([]*string, 0, len(attr));
    for _, i := range attr {
        out = append(out, aws.String(i));
    }
    return out;
}


// deleteSecondaryIndex allows the removal of keys created with createSecondaryIndex
// should only throw an error if the index still exists after we attempted to delete it
// otherwise - don't care
func deleteSecondaryIndex(rt reflect.Type, in string) error {

	// SOME CODE GOES HERE
	return nil
}

//tableHasIndex takes a type and an index name and if an index with
//the given name exsists, it will return a key representing the spec
//of the given key.
//
//TODO_JAPHY - this can be used for testing and as a utility for
//createSecondaryIndex
func tableHasIndex(rt reflect.Type, in string) (key, bool) {

	//YOUR CODE GOES HERE
	return key{}, false
}

//CreateKeyMaker accepts a type, and an index name.
//The index name should specify the name of an index available within
//the provided type.  This method will return a keyMaker capable
//of generating queries on a table (specified by the type), using
//the index referenced by name.
//
//TODO_JAPHY -  Initially let's focues on the simplest case, just a plain
// dynamodb.GetItem query.  While you're working on this think about
// how this may be useful for dynamodb.Scan as well. ( how to specify
// range key more loosely?  How does that work with keymaker / the interfaces
// that consume keymaker? eg. dynago.Get vs dynago.GetAll)
//
// When this is done CreateKeyMaker(rt) simply becomes a special case of this
// method - so most of the code for this will come from there, but be more
// generalized. This method returns no errors - we expect the errors to come
// from the query execution. You'll have to look up the field name for the
// associated attribute values.
func CreateKeyMakerByName(rt reflect.Type, in string) KeyMaker {

	//YOUR CODE GOES HERE.
	return func(...interface{}) (key, error) {
		return key{}, nil
	}
}

//CreateKeyMaker To put items to dynamoDB is one thing (Marshal), but to
// get items from dynamoDB often requires a GetItemInput (if the item is
// fetched by primary key directly) this method will convert a struct i
// with a key value ...k [partition key, rangekey] to a GetItemInput as
// long as the struct is properly tagged, and the partition key and range
// key are of the type descibed by the struct
//
// This method may have some logical overlap with encode()
// should look into that someday.  May just be able to grab the KeySchema?
func CreateKeyMaker(rt reflect.Type) KeyMaker {
	//allow pointers to struct
	var t reflect.Type
	switch rt.Kind() {
	case reflect.Ptr:
		t = rt.Elem()
	default:
		t = rt
	}

	priK := key{
		t:    primary,
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

//AppendToBatchGet
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
