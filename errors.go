package dynaGo

import (
	"reflect"
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
