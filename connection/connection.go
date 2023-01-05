package connection

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//var chConn driver.Conn

func GetConnection() (driver.Conn, error) {
	port := 9000
	host := "127.0.0.1"
	//username := "default"
	//password := "ClickHouse"
	//database := "test_baghel"

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		// Auth: clickhouse.Auth{
		// 	Database: database,
		// },
	})

	fmt.Println("got connection", conn, err)
	return conn, err

}
