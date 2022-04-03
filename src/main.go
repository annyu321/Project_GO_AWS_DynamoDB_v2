package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

//Create structs to hold movies info
type Movie struct {
	Year  int
	Title string
	Info  Info
}

type Info struct {
	Plot      string
	Rank      int
	Rating    float64
	Image_url string
}

var logPath = "./log.txt"
var logFile *os.File

func init() {
	//Log handler
	if logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		log.SetOutput(logFile)
	} else {
		fmt.Println(err)
	}
}

func main() {
	message := make(chan string)
	var tableExist bool

	//Close log file
	defer logFile.Close()

	//Create AWS DynamoDB service object
	svc, err := New()
	if err != nil {
		log.Printf("Eroor in creating dynamodb service object")
	}

	//Check what tables exist in current region
	//Use paginate if there are more than 100 tables
	existTables := List(svc)
	log.Printf("%s", existTables)

	//Create table with Partition key and Sort key
	year, month, date := time.Now().Date()
	tableName := "Movies-Collection-" + strconv.Itoa(year) + "-" + strconv.Itoa(int(month)) + "-" + strconv.Itoa(date)

	//Check if the table name is unique for the AWS account and region
	for _, v := range existTables {
		if tableName == v {
			tableExist = true
			break
		}
	}

	if !tableExist {
		Create(svc, tableName)
	}

	//Wait for table to be activated
	if err := waitForTableActivated(svc, tableName); err != nil {
		log.Printf("%s", err)
	}

	//Use goroutine to Load a json file to the table
	go func() {
		err := LoadData(svc, tableName)
		if err == nil {
			message <- "done"
		} else {
			message <- "issue"
		}
	}()

	//select blocks until one of its cases can run
	select {
	case msg := <-message:
		if msg == "done" {
			//Scan and filter to get items
			//Use FilterExpression and ExpressionAttributeNames properties
			Get(svc, tableName)

			//Query to get a collection of items sharing the same partition key
			//Also use operators for SortKey
			QueryData(svc, tableName)

			//Insert, update or delete multiple items in a single API call
			BatchWrite(svc, tableName)
		} else {
			log.Printf("Error in loading data")
		}
	}
}
