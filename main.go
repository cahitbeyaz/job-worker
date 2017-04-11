package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var (
	MaxWorker       = 3  //os.Getenv("MAX_WORKERS")
	MaxQueue        = 20 //os.Getenv("MAX_QUEUE")
	MaxLength int64 = 2048
)

type PayloadCollection struct {
	WindowsVersion string    `json:"version"`
	Token          string    `json:"token"`
	Payloads       []Payload `json:"data"`
}

type Payload struct {
	// [redacted]
}

func (p *Payload) UploadToS3() error {
	// the storageFolder method ensures that there are no name collision in
	// case we get same timestamp in the key name
	/*
		storage_path := fmt.Sprintf("%v/%v", p.storageFolder, time.Now().UnixNano())

		bucket := S3Bucket

		b := new(bytes.Buffer)
		encodeErr := json.NewEncoder(b).Encode(payload)
		if encodeErr != nil {
			return encodeErr
		}

		// Everything we post to the S3 bucket should be marked 'private'
		var acl = s3.Private
		var contentType = "application/octet-stream"

		return bucket.PutReader(storage_path, b, int64(b.Len()), contentType, acl, s3.Options{})
	*/
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	fmt.Println("work done")
	return nil
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.Payload.UploadToS3(); err != nil {
					log.Printf("Error uploading to S3: %s", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
func payloadHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content = &PayloadCollection{}
	err := json.NewDecoder(io.LimitReader(r.Body, MaxLength)).Decode(&content)
	if err != nil {
		fmt.Errorf("an error occured while deserializing message")
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fmt.Println("A valid payload request received")

	// Go through each payload and queue items individually to be posted to S3
	for _, payload := range content.Payloads {

		// let's create a job with the payload
		work := Job{Payload: payload}
		fmt.Println("sending payload  to workque")
		// Push the work onto the queue.
		JobQueue <- work
		fmt.Println("sent payload  to workque")

	}

	w.WriteHeader(http.StatusOK)
}
func main() {
	JobQueue = make(chan Job, MaxQueue)
	log.Println("main start")
	dispatcher := NewDispatcher(MaxWorker)
	dispatcher.Run()
	http.HandleFunc("/payload/", payloadHandler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("starting listening for payload messages")
	} else {
		fmt.Errorf("an error occured while starting payload server %s", err.Error())
	}

	time.Sleep(time.Hour)
}
