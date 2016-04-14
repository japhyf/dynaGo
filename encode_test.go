// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"encoding/hex"
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
	if err := createTable(svc, Tag{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Usrs'`)
	if err := createTable(svc, Usr{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Sessions'`)
	if err := createTable(svc, Session{}, 1, 1); err != nil {
		t.Error(err)
	}
	t.Log(`create table 'Messages'`)
	if err := createTable(svc, Message{}, 1, 1); err != nil {
		t.Error(err)
	}
}

func TestEncodeValues(t *testing.T) {

	ses0 := Session{
		Id: "abc",
		Mentor: &Usr{
			Id: "bobo",
		},
		Mentee: &Usr{
			Id: "hooch",
		},
		Begin:    time.Now().Unix(),
		End:      time.Now().Unix() + 10,
		Duration: 10,
	}
	ses1 := Session{
		Id: "def",
		Mentor: &Usr{
			Id: "obob",
		},
		Mentee: &Usr{
			Id: "oohhc",
		},
		Begin:    time.Now().Unix(),
		End:      time.Now().Unix() + 10,
		Duration: 10,
	}
	msg := Message{
		Session:   &ses0,
		Id:        "2unique",
		Timestamp: time.Now().Unix(),
		Body:      "it's sweat, what you smell is sweat.",
	}
	tag := Tag{
		Name:     "talkietalk",
		Id:       "123abc",
		Sessions: []*Session{&ses0, &ses1},
		Begin:    ses0.Begin + 1,
		End:      ses1.End - 1,
	}
	b, _ := hex.DecodeString("ab091cf3")
	usr := Usr{
		Id:     "1000",
		Origin: "home",
		Pswd:   b,
		Email:  "guy@home.org",
		Alias:  "guy",
	}

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
	if _, err := svc.PutItem(Marshal(usr)); err != nil {
		t.Errorf("failed: %s", err.Error())
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
	Mentor   *Usr
	Mentee   *Usr
	Begin    int64
	End      int64 `dynaGo:",RANGE"`
	Duration int64
}

type Message struct {
	Session   *Session `dynaGo:"SessionId,HASH"`
	Timestamp int64    `dynaGo:",RANGE"`
	Id        string   `dynaGo:"MessageId"`
	Body      string
}
