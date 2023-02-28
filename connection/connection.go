package connection

import (
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

//var chConn driver.Conn

func GetConnection() (driver.Conn, error) {

	configPath := os.Getenv("CONFIG_PATH")
	fmt.Println("conf path is: ", configPath)
	if configPath == "" {
		fmt.Println("Please provide the config path in env variable 'CONFIG_PATH'. Trying defaults..")
		configPath = "src/configs"
	}
	viper.AddConfigPath(configPath)
	viper.SetConfigName("interactor") // name of config file (without extension)
	viper.SetConfigType("json")       // REQUIRED if the config file does not have the extension in the name
	//viper.AddConfigPath("/Users/pubaghel/Documents/go-workspace/src/clickhouse-golang-interactor/conf/")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	fmt.Println(viper.Get("clickhouse"))

	//port := viper.GetInt("clickhouse.port")
	host := viper.GetStringSlice("clickhouse.host")
	// port := 9000 , 9440
	// host := "127.0.0.1"
	//username := "default"
	//password := "ClickHouse"
	//database := "test_baghel"
	//	address := []string{}
	fmt.Println("username :", viper.GetString("clickhouse.username"))
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: host,
		Auth: clickhouse.Auth{
			Username: viper.GetString("clickhouse.username"),
			Password: viper.GetString("clickhouse.password"),
		},
	})

	fmt.Println("got connection", conn, err)
	return conn, err

}
