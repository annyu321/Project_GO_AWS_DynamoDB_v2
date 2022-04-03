package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/pkg/errors"
)

//Wait for table to be activated
func waitForTableActivated(db *dynamodb.Client, tbn string) error {
	w := dynamodb.NewTableExistsWaiter(db)
	err := w.Wait(context.TODO(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(tbn),
		},
		1*time.Minute,
	)
	if err != nil {
		return errors.Wrap(err, "Time out while waiting for table to become active")
	}
	return nil
}

//Create table with Partition key and Sort key
func Create(db *dynamodb.Client, tbn string) {
	_, err := db.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Rating"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				//Partition key
				AttributeName: aws.String("Year"),
				KeyType:       types.KeyTypeHash,
			},
			{
				//Sort key
				AttributeName: aws.String("Rating"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(tbn),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Println(err)
	}
}

//Load AWS moviedata.json file to the table
func LoadData(db *dynamodb.Client, tbn string) error {
	//Read data from the source file
	var items []Movie
	if data, err := os.ReadFile("./moviedata.json"); err == nil {
		json.Unmarshal(data, &items)
	} else {
		log.Printf("%s", err)
	}

	fmt.Println("Loading data...")
	fmt.Println("This may take a few minutes to complete")
	for _, item := range items {
		log.Println(item.Year)
		log.Println(item.Title)
		log.Println(item.Info.Image_url)

		mi := map[string]types.AttributeValue{
			"Year":     &types.AttributeValueMemberS{Value: strconv.Itoa(item.Year)},
			"Title":    &types.AttributeValueMemberS{Value: item.Title},
			"Plot":     &types.AttributeValueMemberS{Value: item.Info.Plot},
			"Rating":   &types.AttributeValueMemberS{Value: fmt.Sprintf("%f", item.Info.Rating)},
			"Rank":     &types.AttributeValueMemberS{Value: strconv.Itoa(item.Info.Rank)},
			"ImageURL": &types.AttributeValueMemberS{Value: item.Info.Image_url},
		}

		input := &dynamodb.PutItemInput{
			Item:      mi,
			TableName: aws.String(tbn),
		}

		if _, err := db.PutItem(context.TODO(), input); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

//Check if the target table is available in current region
//Paginate through to fetch a complete list
func List(db *dynamodb.Client) []string {
	tables := []string{}
	p := dynamodb.NewListTablesPaginator(db, nil, func(o *dynamodb.ListTablesPaginatorOptions) {
		o.StopOnDuplicateToken = true
	})
	for p.HasMorePages() {
		out, err := p.NextPage(context.TODO())
		if err != nil {
			log.Printf("%s", err)
		}

		for _, tbn := range out.TableNames {
			tables = append(tables, tbn)
		}
	}
	return tables
}

func Get(db *dynamodb.Client, tbn string) {
	//Leverage the "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression" package to
	//compute the FilterExpression and ExpressionAttributeNames properties
	expr, err := expression.NewBuilder().WithFilter(
		expression.And(
			expression.AttributeNotExists(expression.Name("deletedAt")),
			expression.Contains(expression.Name("Rank"), "3"),
		),
	).Build()
	if err != nil {
		panic(err)
	}

	out, err := db.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:                 aws.String(tbn),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	if err != nil {
		log.Printf("%s", err)
	}
	log.Printf("Scan Result")
	log.Printf("%s", out.Items)
}

//Query can return up to 1MB of data
func QueryData(db *dynamodb.Client, tbn string) {
	out, err := db.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(tbn),
		KeyConditionExpression: aws.String("#Year = :hashKey and #Rating > :rangeKey"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":hashKey":  &types.AttributeValueMemberS{Value: "2013"},
			":rangeKey": &types.AttributeValueMemberS{Value: "8.3"},
		},
		ExpressionAttributeNames: map[string]string{
			"#Year":   "Year",
			"#Rating": "Rating",
		},
	})
	if err != nil {
		log.Printf("%s", err)
	}
	log.Printf("Query\n")
	log.Printf("%s", out.Items)
}

//func BatchWrite(ctx context.Context, db *dynamodb.Client, tbn string) {
func BatchWrite(db *dynamodb.Client, tbn string) {
	out, err := db.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tbn: {

				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"Year":   &types.AttributeValueMemberS{Value: "2013"},
							"Rating": &types.AttributeValueMemberS{Value: "8.200000"},
						},
					},
				},

				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"Year":   &types.AttributeValueMemberS{Value: "2014"},
							"Title":  &types.AttributeValueMemberS{Value: "Life After Beth"},
							"Plot":   &types.AttributeValueMemberS{Value: "Plot unknown."},
							"Rank":   &types.AttributeValueMemberS{Value: "4790"},
							"Rating": &types.AttributeValueMemberS{Value: "8.0"},
						},
					},
				},
			},
		},
	})

	if err != nil {
		log.Printf("%s", err)
	}
	log.Println("Batch Insert, update or delete multiple items")
	log.Println("%s", out)
}
