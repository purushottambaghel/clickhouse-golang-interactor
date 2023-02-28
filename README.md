# clickhouse-golang-interactor
This is interactor is build in go for clickhouse. You can create data in clickhouse with the help of this.

need go version go1.18.1

# commands:
```
export GO111MODULE=on
go mod tidy 
```

env GOOS=linux go build -tags static -o interactor main.go

./interactor <numsOfRowsToBeCreated>
argument is optional. Default value is 50 million

# how to run in local
set CONFIG_PATH as path of conf
  ```
go run main.go ct feedsItemFilters
  ```

# how to run in staging
build with:
  ```
env GOOS=linux go build -tags static -o interactor main.go
  ```
push to the interactor binary to ch-ht-follower1
go to 
  ```
cd /home/purushottambaghel/ch-interactor
  ```
copy your config and binary here
give access to binary 
  ```
chmod +777 interactor
  ```
run 
  ```
./interactor --help
  ```

