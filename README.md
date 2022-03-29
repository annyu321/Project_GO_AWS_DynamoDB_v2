#GO AWS DynamoDB Application

## Introduction
This GO AWS DynamoDB Application performs a variety of operations using AWS DynamoDB SDK v2 and Go programming language.

## Functionalities:
1. Connect to AWS DynamoDB or use DynamoDB local to create an AWS DynamoDB service object.

2. Check if the table name is unique for the AWS account and region.

3. Create a table with the Partition key and Sort key. 
   Wait for the newly created table to be activated.

4. Use goroutine to Load the AWS moviedata.json file to the table.

5. Scan and filter to get items.
   Use FilterExpression and ExpressionAttributeNames properties to filter out. 
   
6. Query to get a collection of items sharing the same partition key.
   Use operators for SortKey to get items.
   
7. Use BatchWrite to insert, update or delete multiple items in a single API call.

8. Save information in the log file.

## Setup
- GO 1.18
- "github.com/aws/aws-sdk-go-v2/aws"
- "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
- "github.com/aws/aws-sdk-go-v2/service/dynamodb"
- "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

- Deploy DynamoDB Locally  
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

## Execution
- go run main.go aws.go handler.go

## Test
Use AWS CLI 
- aws dynamodb list-tables --endpoint-url http://localhost:8000
- aws dynamodb delete-table --table-name Movies-Collection-2022-3-29 --endpoint-url http://localhost:8000
- aws dynamodb scan --table-name Movies-Collection-2022-3-29 --endpoint-url http://localhost:8000
