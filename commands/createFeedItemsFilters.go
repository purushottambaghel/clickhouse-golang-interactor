package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"example.com/m/connection"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cmdFeedsItemFilters = &cobra.Command{
	Use:     "feedsItemFilters",
	Version: "1.0.0",
	Short:   "create feeds item table",
	Long:    `create feeds item table and load data to clickhouse. feeds item will have following primary key: account_id,feed_type, object_id, event_timestamp_in_ms. It has filters.`,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := connection.GetConnection()
		fmt.Println("got connection", conn, err)
		//loadingDataForAccountId(conn)
		insertDataForAccountId(conn)
	},
}

func loadingDataForAccountId(conn driver.Conn) error {

	fmt.Println("starting table creation")

	if err := conn.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS mockfeeds"); err != nil {
		fmt.Println("error:", err)
		return err
	}

	//fmt.Println(conn.Exec(context.Background(), "show tables"))

	if err := conn.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS mockfeeds.feeds_items_account_id
	(
		feed_type String,
		event_timestamp_in_ms DateTime64(3, 'UTC'),
		object_id UInt64,
		account_id UInt64,
		data String,
		filters Map(String, Array(String))
	)
	ENGINE = MergeTree
	PRIMARY KEY ( account_id,feed_type, object_id, event_timestamp_in_ms)
	ORDER BY ( account_id,feed_type, object_id, event_timestamp_in_ms)
	`); err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("finish table creation")
	insertDataForAccountId(conn)
	return nil
}

func insertDataForAccountId(conn driver.Conn) error {

	//service: freshrelease,freshbots,freshdesk,freshsales, freshidv2, freshmarketer, freshinbox, freshchat, ubx
	//feeds_type: audit, ticket ,change,alert_log, asset, problem, workflow, alert, import_log, Issue, TestCase
	// account_id: 45035350216713, 590905
	//object_id: 2043139031129, 28004040077
	//conn, err := GetNativeConnection(nil, nil, nil)
	fmt.Println("in query row feeds item")

	services := [10]string{"freshrelease", "freshbots", "freshdesk", "freshsales", "freshidv2", "freshmarketer", "freshinbox", "freshchat", "ubx", "wallet"}
	feedTypes := [10]string{"audit", "ticket", "change", "alert_log", "asset", "problem", "workflow", "alert", "import_log", "Issue"}
	jsonstr := `{"actor_id":["186032035843863143"],
	"vendor_event_type": [
	  "SUBSCRIPTION_CANCELLED"
	],
	"subscription_id": [
	  "319409678036779089"
	],
	"action": [
	  "SUBSCRIPTION_CANCELLED"
	]}`
	m := make(map[string][]string)
	json.Unmarshal([]byte(jsonstr), &m)
	numServices := len(services)
	numFeedTypes := len(feedTypes)
	batchSize := 1000
	rowsInOneIteration := numServices * numFeedTypes * batchSize

	var totalRequiredRows int = viper.GetInt("totalRequiredRows")
	fmt.Println("total rows to be created ", totalRequiredRows)
	if totalRequiredRows == 0 {
		fmt.Println("in if")
		totalRequiredRows = 500000
	}

	totalIteration := totalRequiredRows / rowsInOneIteration
	var account_id uint64 = 590905
	var object_id uint64 = 28004040077
	numberOfRowscreated := 0
	for i := 1; i <= totalIteration; i++ {
		//this will produce 100000 rows
		//	filters:=
		account_id++
		for _, service := range services {
			//fmt.Println(service)

			for _, feedType := range feedTypes {
				//fmt.Println(feedType)

				object_id++

				//today := time.Now().UnixMilli()
				start := time.Date(
					2022, 9, 17, 20, 34, 58, 651387237, time.UTC).UnixMilli()

				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO mockfeeds.feeds_items_filters_part")
				if err != nil {
					fmt.Println(err)
					return err
				}

				for i := 0; i < batchSize; i++ {
					start += 400000
					if err := batch.Append(
						feedType,
						start,
						object_id,
						account_id,
						"hello"+service,
						m,

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
