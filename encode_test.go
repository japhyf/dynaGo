// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

/*
var TablesSchema = map[string]*dynamodb.CreateTableInput{
	"Messages": &dynamodb.CreateTableInput{
		TableName: aws.String("Messages"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("SessionId"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String("Timestamp"),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("SessionId"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Timestamp"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeN),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	},
	"Usrs": &dynamodb.CreateTableInput{
		TableName: aws.String("Usrs"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("UserId"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("UserId"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Origin"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("ByOrigin"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("Origin"),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeKeysOnly),
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	},
	"Tags": &dynamodb.CreateTableInput{
		TableName: aws.String("Tags"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("TagId"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String("Timestamp"),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("TagId"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Timestamp"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeN),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	},
}
*/
var cred = &credentials.SharedCredentialsProvider{Profile: "admin_marcus"}
var svc = dynamodb.New(
	session.New(),
	&aws.Config{
		Credentials: credentials.NewCredentials(cred),
		Endpoint:    aws.String("http://localhost:8000"),
		Region:      aws.String("us-east-1"),
	})

func TestEncodeTables(t *testing.T) {
	t.Log(`create table 'Tags'`)
	if err := CreateTable(svc, Tag{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Usrs'`)
	if err := CreateTable(svc, Usr{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Sessions'`)
	if err := CreateTable(svc, Session{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Messages'`)
	if err := CreateTable(svc, Message{}, 1, 1); err != nil {
		t.Error(err)
	}
}

var (
	ses0 = Session{
		Id: "abc",
		Admin: &Usr{
			Id: "bobo",
		},
		Begin:    time.Now().Unix(),
		End:      time.Now().Unix() + 10,
		Duration: 10,
	}
	ses1 = Session{
		Id: "def",
		Admin: &Usr{
			Id: "obob",
		},
		Begin:    time.Now().Unix(),
		End:      time.Now().Unix() + 10,
		Duration: 10,
	}
	msg = Message{
		Session:   &ses0,
		Id:        "2unique",
		Timestamp: time.Now().Unix(),
		Body:      "it's sweat, what you smell is sweat.",
	}
	tag = Tag{
		Name:     "talkietalk",
		Id:       "123abc",
		Sessions: []*Session{&ses0, &ses1},
		Begin:    ses0.Begin + 1,
		End:      ses1.End - 1,
	}
	b0, _ = hex.DecodeString("ab091cf3")
	b1, _ = hex.DecodeString("aefc0e24")
	usr0  = Usr{
		Id:     "1000",
		Origin: "",
		Pswd:   b0,
		Email:  "bob@home.org",
		Alias:  "bob",
	}
	usr1 = Usr{
		Id:     "2000",
		Origin: "",
		Pswd:   b1,
		Email:  "alice@home.org",
		Alias:  "alice",
	}
	sum0 = SessionUsrMap{
		Usr: &Usr{
			Id: "1000",
		},
		Session: &Session{
			Id: "abc",
		},
	}

	sum1 = SessionUsrMap{
		Usr: &Usr{
			Id: "2000",
		},
		Session: &Session{
			Id: "abc",
		},
	}
)

func TestEncodeValues(t *testing.T) {

	t.Log("Put message...")
	if _, err := svc.PutItem(Marshal(msg)); err != nil {
		t.Errorf("failed: %s", err.Error())
	}
	t.Log("Put session...")
	if _, err := svc.PutItem(Marshal(ses0)); err != nil {
		t.Errorf("failed: %s", err.Error())
	}
	t.Log("Put tag...")
	if _, err := svc.PutItem(Marshal(tag)); err != nil {
		t.Errorf("failed: %s", err.Error())
	}
	t.Log("Put usr...")
	if _, err := svc.PutItem(Marshal(usr0)); err != nil {
		t.Errorf("usr0 failed: %s", err.Error())
	}
	if _, err := svc.PutItem(Marshal(usr1)); err != nil {
		t.Errorf("usr1 failed: %s", err.Error())
	}
	t.Log("Put sum...")
	if _, err := svc.PutItem(Marshal(sum0)); err != nil {
		t.Errorf("sum0 failed: %s", err.Error())
	}
	if _, err := svc.PutItem(Marshal(sum1)); err != nil {
		t.Errorf("sum1 failed: %s", err.Error())
	}
}
func TestGetValues(t *testing.T) {
	t.Log("Get usr0...")
	tryGetValue(t, Usr{}, usr0, "1000")
	t.Log("Get usr1...")
	tryGetValue(t, Usr{}, usr1, "2000")
	t.Log("Get ses..")
	tryGetValue(t, Session{}, ses0, "abc")
	t.Log("Get sum.")
	tryGetValue(t, SessionUsrMap{}, sum0, usr0.Id, ses0.Id)
}

func tryGetValue(t *testing.T, i interface{}, v interface{}, k ...interface{}) {
	gi, _ := AsGetItemInput(i, k...)
	resp, err := svc.GetItem(gi)
	if err != nil {
		t.Errorf("failed: %s", err.Error())
	}
	if len(resp.Item) < 1 {
		t.Errorf("failed: response for GetItem was incorrect length")
	}
	//construct new ptr
	u := reflect.New(reflect.TypeOf(i)).Interface()
	if err := Unmarshal(resp.Item, u); err != nil {
		t.Errorf("failed: %s", err.Error())
	}
	//dereference pointer
	e := reflect.ValueOf(u).Elem().Interface()
	if !reflect.DeepEqual(e, v) {
		b0, _ := json.Marshal(e)
		b1, _ := json.Marshal(v)
		t.Error(fmt.Sprintf("failed: GetItem response not equal to original item \n\t %T %s \n\t %T %s",
			v, string(b1), e, string(b0)))
	}

}

type Tag struct {
	Name     string `dynaGo:",HASH"`
	Id       string `dynaGo:"TagId"`
	Sessions []*Session
	Begin    int64
	End      int64 `dynaGo:",RANGE"`
}

type Usr struct {
	Id     string `dynaGo:"UserId,HASH"`
	Origin string
	Pswd   []byte
	Email  string
	Alias  string
}

type Session struct {
	Id       string `dynaGo:"SessionId,HASH"`
	Admin    *Usr
	Begin    int64
	End      int64
	Duration int64
}

type SessionUsrMap struct {
	Usr     *Usr     `dynaGo:",HASH"`
	Session *Session `dynaGo:",RANGE"`
}
type Message struct {
	Session   *Session `dynaGo:"SessionId,HASH"`
	Timestamp int64    `dynaGo:",RANGE"`
	Id        string   `dynaGo:"MessageId"`
	Body      string
}
