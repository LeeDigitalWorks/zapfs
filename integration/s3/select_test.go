//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"strings"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Select(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("select from CSV file", func(t *testing.T) {
		bucket := uniqueBucket("test-select-csv")
		key := uniqueKey("data.csv")
		csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
Diana,28,Houston`

		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Upload CSV file
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(csvData),
			ContentType: aws.String("text/csv"),
		})
		require.NoError(t, err)

		// Select all records
		resp, err := rawClient.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String(key),
			Expression:     aws.String("SELECT * FROM s3object"),
			ExpressionType: s3types.ExpressionTypeSql,
			InputSerialization: &s3types.InputSerialization{
				CSV: &s3types.CSVInput{
					FileHeaderInfo: s3types.FileHeaderInfoUse,
				},
			},
			OutputSerialization: &s3types.OutputSerialization{
				CSV: &s3types.CSVOutput{},
			},
		})
		require.NoError(t, err)
		defer resp.GetStream().Close()

		// Read the response stream
		var results strings.Builder
		for event := range resp.GetStream().Events() {
			switch v := event.(type) {
			case *s3types.SelectObjectContentEventStreamMemberRecords:
				results.Write(v.Value.Payload)
			}
		}

		// Verify results contain expected data
		output := results.String()
		assert.Contains(t, output, "Alice")
		assert.Contains(t, output, "Bob")
		assert.Contains(t, output, "Charlie")
		assert.Contains(t, output, "Diana")
	})

	t.Run("select with WHERE clause", func(t *testing.T) {
		bucket := uniqueBucket("test-select-where")
		key := uniqueKey("data.csv")
		csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago
Diana,28,Houston`

		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Upload CSV file
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(csvData),
			ContentType: aws.String("text/csv"),
		})
		require.NoError(t, err)

		// Select records where age > 28
		resp, err := rawClient.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String(key),
			Expression:     aws.String("SELECT name, age FROM s3object WHERE CAST(age AS INT) > 28"),
			ExpressionType: s3types.ExpressionTypeSql,
			InputSerialization: &s3types.InputSerialization{
				CSV: &s3types.CSVInput{
					FileHeaderInfo: s3types.FileHeaderInfoUse,
				},
			},
			OutputSerialization: &s3types.OutputSerialization{
				CSV: &s3types.CSVOutput{},
			},
		})
		require.NoError(t, err)
		defer resp.GetStream().Close()

		// Read the response stream
		var results strings.Builder
		for event := range resp.GetStream().Events() {
			switch v := event.(type) {
			case *s3types.SelectObjectContentEventStreamMemberRecords:
				results.Write(v.Value.Payload)
			}
		}

		// Should only have Alice (30) and Charlie (35)
		output := results.String()
		assert.Contains(t, output, "Alice")
		assert.Contains(t, output, "Charlie")
		assert.NotContains(t, output, "Bob")   // age 25
		assert.NotContains(t, output, "Diana") // age 28
	})

	t.Run("select from JSON file", func(t *testing.T) {
		bucket := uniqueBucket("test-select-json")
		key := uniqueKey("data.json")
		jsonData := `{"name": "Alice", "age": 30, "city": "New York"}
{"name": "Bob", "age": 25, "city": "Los Angeles"}
{"name": "Charlie", "age": 35, "city": "Chicago"}`

		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Upload JSON lines file
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(jsonData),
			ContentType: aws.String("application/json"),
		})
		require.NoError(t, err)

		// Select from JSON
		resp, err := rawClient.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String(key),
			Expression:     aws.String("SELECT s.name, s.age FROM s3object s"),
			ExpressionType: s3types.ExpressionTypeSql,
			InputSerialization: &s3types.InputSerialization{
				JSON: &s3types.JSONInput{
					Type: s3types.JSONTypeLines,
				},
			},
			OutputSerialization: &s3types.OutputSerialization{
				JSON: &s3types.JSONOutput{},
			},
		})
		require.NoError(t, err)
		defer resp.GetStream().Close()

		// Read the response stream
		var results strings.Builder
		for event := range resp.GetStream().Events() {
			switch v := event.(type) {
			case *s3types.SelectObjectContentEventStreamMemberRecords:
				results.Write(v.Value.Payload)
			}
		}

		// Verify results contain expected data
		output := results.String()
		assert.Contains(t, output, "Alice")
		assert.Contains(t, output, "Bob")
		assert.Contains(t, output, "Charlie")
	})

	t.Run("select with LIMIT", func(t *testing.T) {
		bucket := uniqueBucket("test-select-limit")
		key := uniqueKey("data.csv")
		csvData := `id,value
1,one
2,two
3,three
4,four
5,five`

		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Upload CSV file
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(csvData),
		})
		require.NoError(t, err)

		// Select with LIMIT
		resp, err := rawClient.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String(key),
			Expression:     aws.String("SELECT * FROM s3object LIMIT 2"),
			ExpressionType: s3types.ExpressionTypeSql,
			InputSerialization: &s3types.InputSerialization{
				CSV: &s3types.CSVInput{
					FileHeaderInfo: s3types.FileHeaderInfoUse,
				},
			},
			OutputSerialization: &s3types.OutputSerialization{
				CSV: &s3types.CSVOutput{},
			},
		})
		require.NoError(t, err)
		defer resp.GetStream().Close()

		// Read the response stream
		var results strings.Builder
		for event := range resp.GetStream().Events() {
			switch v := event.(type) {
			case *s3types.SelectObjectContentEventStreamMemberRecords:
				results.Write(v.Value.Payload)
			}
		}

		// Should only have first 2 records
		output := results.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.LessOrEqual(t, len(lines), 2, "should have at most 2 records")
	})

	t.Run("select non-existent object", func(t *testing.T) {
		bucket := uniqueBucket("test-select-404")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Select from non-existent object
		_, err := rawClient.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            aws.String("non-existent.csv"),
			Expression:     aws.String("SELECT * FROM s3object"),
			ExpressionType: s3types.ExpressionTypeSql,
			InputSerialization: &s3types.InputSerialization{
				CSV: &s3types.CSVInput{},
			},
			OutputSerialization: &s3types.OutputSerialization{
				CSV: &s3types.CSVOutput{},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NoSuchKey")
	})
}
