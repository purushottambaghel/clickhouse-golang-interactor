// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {

	port := 9000
	host := "127.0.0.1"
	//username := "default"
	//password := "ClickHouse"
	database := "test_baghel"

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
		},
	})

	//QueryRow(conn)
	err = CreateTable(conn)
	err = InsertRows(conn)

	if err != nil {
		fmt.Println("err1", err)
	}
	// v, err := conn.ServerVersion()
	// fmt.Println("version", v)
	// if err != nil {
	// 	fmt.Println("err2", err)
	// }
}

func CreateTable(conn driver.Conn) error {

	fmt.Println("in create table")
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_feeds_item")

	//fmt.Println(conn.Exec(context.Background(), "show tables"))

	if err := conn.Exec(context.Background(), `
	CREATE TABLE test_feeds_item
	(
		service_id String,
		feed_type String,
		event_timestamp_in_ms DateTime64(3, 'UTC'),
		object_id UInt64,
		account_id UInt64,
		data String
	)
	ENGINE = MergeTree
	PRIMARY KEY (service_id, feed_type, account_id, object_id, event_timestamp_in_ms)
	ORDER BY (service_id, feed_type, account_id, object_id, event_timestamp_in_ms)
	`); err != nil {
		return err
	}
	return nil
}

func InsertRows(conn driver.Conn) error {

	//service: freshrelease,freshbots,freshdesk,freshsales, freshidv2, freshmarketer, freshinbox, freshchat, ubx
	//feeds_type: audit, ticket ,change,alert_log, asset, problem, workflow, alert, import_log, Issue, TestCase
	// account_id: 45035350216713, 590905
	//object_id: 2043139031129, 28004040077
	//conn, err := GetNativeConnection(nil, nil, nil)
	fmt.Println("in query row feeds item")
	services := [10]string{"freshrelease", "freshbots", "freshdesk", "freshsales", "freshidv2", "freshmarketer", "freshinbox", "freshchat", "ubx", "wallet"}
	feedTypes := [10]string{"audit", "ticket", "change", "alert_log", "asset", "problem", "workflow", "alert", "import_log", "Issue"}

	rowsInOneIteration := 100000
	totalRequiredRows := 5000000
	totalIteration := totalRequiredRows / rowsInOneIteration
	var account_id uint64 = 590905
	var object_id uint64 = 28004040077
	numberOfRowscreated := 0
	for i := 1; i <= totalIteration; i++ {
		//this will produce 100000 rows

		for _, service := range services {
			//fmt.Println(service)

			account_id++

			for _, feedType := range feedTypes {
				//fmt.Println(feedType)

				object_id++

				//today := time.Now().UnixMilli()
				start := time.Date(
					2022, 9, 17, 20, 34, 58, 651387237, time.UTC).UnixMilli()

				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_feeds_item")
				if err != nil {
					fmt.Println(err)
					return err
				}

				for i := 0; i < 1000; i++ {
					start += 400000
					if err := batch.Append(
						service,
						feedType,
						start,
						object_id,
						account_id,
						"hello",
						//"adsfhashfkajhfkadshfasdhfksnkladsjfdasjfksdljfkldajsfkldsajflksdjfksnfkldjfkdjflkfhdflkndfkljdshfoisdnfklsdhfnckdfsdfhlkdsfjklFDJifjiLFJLIFHRFDSVNVHSAFHKDJFKLSJFLKSAFJAOIFJSDKLJVMCSCNSAKDCHUADFHSDJDNCDSKAFHUFHIORWJOsdjkasdnfkljosakdflkajsfioashfknckanfuisadfncndsjklajdlkajfoapfjcasmlkfsaoifnaf;apahdfhaisdfhknkdfjioadshfisdnkcsndhdifhadiofsdalkjkasjdflajioriidkfnsdakhiafjkcnfhnkcndsfhidfjsldjfoiasdhjfwkndnkfjbaifaljfaljfailnfjdsiofnadsuifjkdnfdsafudfuiasdufnadsknfasdjpnfsfnsndsknddfdjsfidffisdnfd", //500 bytes data
					); err != nil {
						fmt.Println(err)
						return err
					}
				}
				if err := batch.Send(); err != nil {
					fmt.Println(err)
					return err
				}

			}
		}
		numberOfRowscreated += rowsInOneIteration
		fmt.Println("Number of rows created: ", numberOfRowscreated)
	}
	return nil
}
