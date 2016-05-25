// Copyright 2016 Appittome. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dynaGo

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Don't think this test will ever fail unless someone panics.
func TestDecode(t *testing.T) {
	cred := &credentials.SharedCredentialsProvider{Profile: "admin_marcus"}
	svc := dynamodb.New(
		session.New(),
		&aws.Config{
			Credentials: credentials.NewCredentials(cred),
			Endpoint:    aws.String("http://localhost:8000"),
			Region:      aws.String("us-east-1"),
		})

	//pointer to session
	msgs := exercise(t, svc, Message{}).([]*Message)
	for _, msg := range msgs {
		t := time.Unix(msg.Timestamp, 0)
		fmt.Println(t.String() + ` [ ` + msg.Id + ` ] "` + msg.Body + `" ` + msg.Session.Id)
	}
	fmt.Printf("----- Message count:%d\n\n", len(msgs))

	// array of pointers to session structs
	tags := exercise(t, svc, Tag{}).([]*Tag)
	for _, tag := range tags {
		b := time.Unix(tag.Begin, 0)
		e := time.Unix(tag.End, 0)
		fmt.Printf("\"%s\" [%s] %s => %s", tag.Name, tag.Id, b, e)
		for _, ses := range tag.Sessions {
			fmt.Printf(" %s", ses.Id)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("----- Tag count:%d\n\n", len(tags))

	// pointers to user structs
	sess := exercise(t, svc, Session{}).([]*Session)
	for _, ses := range sess {
		b := time.Unix(ses.Begin, 0)
		e := time.Unix(ses.End, 0)
		fmt.Printf("[%s] %s talks to %s\n", ses.Id, ses.Mentee.Id, ses.Mentor.Id)
		fmt.Printf("\t %s => %s :: %d\n", b, e, ses.Duration)
	}
	fmt.Printf("----- Session count:%d\n\n", len(sess))

	// all strings.. not too intersting.
	users := exercise(t, svc, Usr{}).([]*Usr)
	for _, user := range users {
		fmt.Printf("%s %s %s %s [% x]\n", user.Id, user.Alias, user.Email, user.Origin, user.Pswd)
	}
	fmt.Printf("----- User count:%d\n\n", len(users))

}

// dynamodb.Scans table.  First page is returned as an array of pointers of the
// type of the interface passed in.  eg exercise(t,svc, Usr{}) returns []*Usr
func exercise(t *testing.T, svc *dynamodb.DynamoDB, i interface{}) interface{} {
	param := &dynamodb.ScanInput{
		TableName: aws.String(TableName(i)),
	}

	resp, err := svc.Scan(param)
	if err != nil {
		t.Error(err)
	}
	l, rt := len(resp.Items), reflect.TypeOf(i)
	items := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(rt)), l, l)
	for n, item := range resp.Items {
		o := reflect.New(rt)
		if err = Unmarshal(item, o.Interface()); err != nil {
			t.Error(err)
		}
		items.Index(n).Set(o)
	}
	return items.Interface()
}
