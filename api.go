package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/mux"
)

func main() {
	fmt.Println("STARTING API LOADER...")

	r := mux.NewRouter()
	// Routes consist of a path and a handler function.
	r.HandleFunc("/", HomeHandler)

	r.HandleFunc("/api/create/sqs", CreateSQSHandler)
	r.HandleFunc("/api/receive/sqs", ReceiveSQSHandler)

	// Bind to a port and pass our router in
	log.Fatal(http.ListenAndServe("localhost:8080", r))
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Welcome to API!\n"))
}

func CreateSQSHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("RECEIVING...")

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	// URL to our queue
	qURL := "https://sqs.ap-southeast-1.amazonaws.com/799216407651/test"

	result, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(0),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"Title": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("The Whistler"),
			},
			"Author": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("John Grisham"),
			},
			"WeeksOn": &sqs.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String("6"),
			},
		},
		MessageBody: aws.String("Data Dump"),
		QueueUrl:    &qURL,
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	fmt.Println("Success", *result.MessageId)

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	// io.WriteString(w, `{"result": true}`)
	io.WriteString(w, *result.MessageId)
}

func ReceiveSQSHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("PROCESSING...")

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	qURL := "https://sqs.ap-southeast-1.amazonaws.com/799216407651/test"

	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &qURL,
		MaxNumberOfMessages: aws.Int64(1),
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

	resultDelete, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &qURL,
		ReceiptHandle: result.Messages[0].ReceiptHandle,
	})

	if err != nil {
		fmt.Println("Delete Error", err)
		return
	}

	fmt.Println("Message Deleted", resultDelete)

	// w.WriteHeader(http.StatusOK)
	// w.Header().Set("Content-Type", "application/json")
	// // io.WriteString(w, `{"result": true}`)
	// io.WriteString(w, resultDelete)
}
