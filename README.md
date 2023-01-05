# clickhouse-golang-interactor

need go version go1.18.1

# commands:
export GO111MODULE=on
go mod tidy 

env GOOS=linux go build -tags static -o interactor main.go

./interactor <numsOfRowsToBeCreated>
argument is optional. Default value is 50 million

