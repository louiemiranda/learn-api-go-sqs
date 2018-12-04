package main

import (
	"fmt"

	"database/sql"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"log"

	_ "github.com/go-sql-driver/mysql"
)

const (
	DriverName     = "mysql"
	DataSourceName = "root:password@/queue"
	MaxMessages    = 1
	QueueEndpoint  = "https://sqs.ap-southeast-1.amazonaws.com/799216407651/test"
)

func main() {

	fmt.Println("WORKER PROCESSOR...")

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	qURL := QueueEndpoint

	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &qURL,
		MaxNumberOfMessages: aws.Int64(MaxMessages),
		VisibilityTimeout:   aws.Int64(20), // 20 seconds
		WaitTimeSeconds:     aws.Int64(0),
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}
	if len(result.Messages) == 0 {
		fmt.Println("Received no messages")
		return
	}

	requestId := *result.Messages[0].MessageId
	data := *result.Messages[0].Body

	_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &qURL,
		ReceiptHandle: result.Messages[0].ReceiptHandle,
	})

	if err != nil {
		fmt.Println("Delete Error", err)
		return
	}

	db, err := sql.Open(DriverName, DataSourceName)

	// @TODO -- mysql, update status of request

	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	stmt, err := db.Prepare("update transactions set status = ?,data = ? where reference = ?")
	if err != nil {
		log.Fatal(err)
	}

	//execute query
	stmt.Exec("completed", data, requestId)

	fmt.Println("Request Id Message Processed", requestId)
}
