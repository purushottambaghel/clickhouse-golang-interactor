package connection

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

//var chConn driver.Conn

func GetConnection() (driver.Conn, error) {

	viper.SetConfigName("interactor") // name of config file (without extension)
	viper.SetConfigType("json")       // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/Users/pubaghel/Documents/go-workspace/src/clickhouse-golang-interactor/conf/")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	fmt.Println(viper.Get("clickhouse"))

	port := viper.GetInt("clickhouse.port")
	host := viper.GetString("clickhouse.host")
	// port := 9000
	// host := "127.0.0.1"
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
