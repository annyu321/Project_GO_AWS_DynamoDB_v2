package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const REGION = "ca-central-1"

func New() (*dynamodb.Client, error) {
	//Connect to AWS DynamoDB
	/*
		cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
			o.Region = REGION
			return nil
		})
	*/

	//Use DynamoDB local
	cfg, err := config.LoadDefaultConfig(context.TODO())

	//Log config error
	if err != nil {
		log.Printf("%s", err)
	}

	//Create AWS DynamoDB service object
	//svc := dynamodb.NewFromConfig(cfg)

	//Create AWS DynamoDB service object and access DynamoDB running locally
	svc := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.EndpointResolver = dynamodb.EndpointResolverFromURL("http://localhost:8000")
	})
	return svc, err
}
