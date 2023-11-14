package main

import (
	"log"
	"time"

	"github.com/myzhan/boomer"
//    "saxml/client/go/sax"
)

func foo(){
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)
    /*
    Report your test result as a success, if you write it in locust, it will looks like this
    events.request_success.fire(request_type="http", name="foo", response_time=100, response_length=10)
    */
    globalBoomer.RecordSuccess("http", "foo", elapsed.Nanoseconds()/int64(time.Millisecond), int64(10))
}

func lm_generate() {
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)

    /*
    Report your test result as a success, if you write it in locust, it will looks like this
    events.request_success.fire(request_type="http", name="foo", response_time=100, response_length=10)
    */
    globalBoomer.RecordSuccess("saxml.client", "lm.Generate", elapsed.Nanoseconds()/int64(time.Millisecond), int64(10)) 
}

var globalBoomer *boomer.Boomer

func main(){
    log.SetFlags(log.LstdFlags | log.Lshortfile)

    task1 := &boomer.Task{
        Name: "foo",
        // The weight is used to distribute goroutines over multiple tasks.
        Weight: 10,
        Fn: foo,
    }

    task2 := &boomer.Task{
        Name: "lm.Generate",
        // The weight is used to distribute goroutines over multiple tasks.
        Weight: 10,
        Fn: lm_generate,
    }


    numClients := 2
	spawnRate := float64(1)
	globalBoomer = boomer.NewStandaloneBoomer(numClients, spawnRate)
	globalBoomer.AddOutput(boomer.NewConsoleOutput()) 
    // Start tasks
    globalBoomer.Run(task1, task2)
}
