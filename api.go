package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/mux"
)

func main() {
	fmt.Println("STARTING API LOADER...")

	r := mux.NewRouter()

	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/api/sqs", CreateSQSHandler).
		Methods("POST")
	r.HandleFunc("/api/sqs/{id}", StatusHandler).
		Methods("GET")

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

	db, err := sql.Open("mysql", "root:password@/queue")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

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

	// INSERT TO DATABASE
	stmt, err := db.Prepare("INSERT INTO transactions (reference, date_created) VALUES(?, NOW())")
	if err != nil {
		log.Fatal(err)
	}
	res, err := stmt.Exec(*result.MessageId)
	if err != nil {
		log.Fatal(err)
	}
	lastId, err := res.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ID = %d, affected = %d\n", lastId, rowCnt)

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	// io.WriteString(w, `{"result": true}`)
	io.WriteString(w, *result.MessageId)
}

func StatusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("VERIFYING STATUS...")

	// QUERY FROM DATABASE

	db, err := sql.Open("mysql", "root:password@/queue")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	var reference string
	// name := "da4cb85a-01cf-42d9-a67a-e7b64ffa7401"
	// err = db.QueryRow("select * from transactions where reference = ?", 1).Scan(&name)
	err = db.QueryRow("select reference from transactions where reference = ?", 1).Scan("da4cb85a-01cf-42d9-a67a-e7b64ffa7401")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reference)

	// w.WriteHeader(http.StatusOK)
	// w.Header().Set("Content-Type", "application/json")
	// // io.WriteString(w, `{"result": true}`)
	// io.WriteString(w, resultDelete)
}
