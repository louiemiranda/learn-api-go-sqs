package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"

	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/mux"
)

const (
	DriverName     = "mysql"
	DataSourceName = "root:password@/queue"
	QueueEndpoint  = "https://sqs.ap-southeast-1.amazonaws.com/799216407651/test"
)

func main() {
	fmt.Println("STARTING API LOADER...")

	r := mux.NewRouter()

	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/api/sqs", CreateSQSHandler).
		Methods("POST")
	r.HandleFunc("/api/sqs", StatusHandler).
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

	status := r.FormValue("status")

	if status == "" {
		err := errors.New("All are mandatory fields")
		log.Fatal(err)
		return
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	db, err := sql.Open(DriverName, DataSourceName)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	// URL to our queue
	qURL := QueueEndpoint

	result, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(0),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"status": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(status),
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
	stmt, err := db.Prepare("INSERT INTO transactions (reference, date_created, status) VALUES(?, NOW(), 'pending')")
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

	requestId := r.FormValue("reference")

	if requestId == "" {
		err := errors.New("Reference is mandatory field")
		fmt.Println(err)
		return
	}

	fmt.Println("VERIFYING STATUS...")

	// QUERY FROM DATABASE

	db, err := sql.Open(DriverName, DataSourceName)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	var (
		id        int
		reference string
		status    string
	)
	rows, err := db.Query("select id, reference, status from transactions where reference = ?", requestId)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &reference, &status)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, reference)

		w.WriteHeader(http.StatusOK)
		// w.Header().Set("Content-Type", "application/json")
		// io.WriteString(w, `{"result": true}`)
		io.WriteString(w, reference)
		io.WriteString(w, ": "+status)

	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
