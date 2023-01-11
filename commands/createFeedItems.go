package commands

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"example.com/m/connection"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/cobra"
)

var cmdFeedsItem = &cobra.Command{
	Use:     "feedsItem",
	Version: "1.0.0",
	Short:   "create feeds item table",
	Long:    `create feeds item table and load data to clickhouse. feeds item will have following primary key: account_id,feed_type, object_id, event_timestamp_in_ms. `,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := connection.GetConnection()
		fmt.Println("got connection", conn, err)
		CreateTableWOServiceIdTable(conn)
	},
}

func CreateTableWOServiceIdTable(conn driver.Conn) error {

	fmt.Println("in create table")
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_feeds_item_wo_service")

	//fmt.Println(conn.Exec(context.Background(), "show tables"))

	if err := conn.Exec(context.Background(), `
	CREATE TABLE test_feeds_item_wo_service
	(
		feed_type String,
		event_timestamp_in_ms DateTime64(3, 'UTC'),
		object_id UInt64,
		account_id UInt64,
		data String
	)
	ENGINE = MergeTree
	PRIMARY KEY (feed_type, account_id, object_id, event_timestamp_in_ms)
	ORDER BY (feed_type, account_id, object_id, event_timestamp_in_ms)
	`); err != nil {
		return err
	}
	InsertRowsInWOServiceIdTable(conn)
	return nil
}

func InsertRowsInWOServiceIdTable(conn driver.Conn) error {

	//service: freshrelease,freshbots,freshdesk,freshsales, freshidv2, freshmarketer, freshinbox, freshchat, ubx
	//feeds_type: audit, ticket ,change,alert_log, asset, problem, workflow, alert, import_log, Issue, TestCase
	// account_id: 45035350216713, 590905
	//object_id: 2043139031129, 28004040077
	//conn, err := GetNativeConnection(nil, nil, nil)
	fmt.Println("in query row feeds item")

	services := [10]string{"freshrelease", "freshbots", "freshdesk", "freshsales", "freshidv2", "freshmarketer", "freshinbox", "freshchat", "ubx", "wallet"}
	feedTypes := [10]string{"audit", "ticket", "change", "alert_log", "asset", "problem", "workflow", "alert", "import_log", "Issue"}

	numServices := len(services)
	numFeedTypes := len(feedTypes)
	batchSize := 1000
	rowsInOneIteration := numServices * numFeedTypes * batchSize
	totalRequiredRows := 5000000
	if len(os.Args) > 1 {
		rows := os.Args[1]
		i, err := strconv.Atoi(rows)
		if err != nil {
			// ... handle error
			panic(err)
		}
		totalRequiredRows = i
	}
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

				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_feeds_item_wo_service")
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
