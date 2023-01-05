#!/bin/sh

echo "hello"

path=$(pwd)
echo $path
cd /Users/pubaghel
# ./clickhouse benchmark --query "SELECT data FROM test_baghel.test_feeds_item
#                          WHERE (service_id = 'freshbots') AND (feed_type = 'alert') AND
#                           (account_id = '591317') AND (object_id = '28004044195') AND 
#                           (event_timestamp_in_ms > '2022-09-21 19:14:58.651') AND 
#                           (event_timestamp_in_ms < '2022-09-22 11:28:18.651')" -i 100 

./clickhouse benchmark -i 100 --json=$path/report < $path/query_file
echo "query" >> $path/report
cat $path/report | grep 95 >> $path/report
#./clickhouse benchmark -i 100 <<< "SELECT data FROM test_baghel.test_feeds_item WHERE (service_id = 'freshbots') AND (feed_type = 'alert') AND (account_id = '591317') AND (object_id = '28004044195') AND (event_timestamp_in_ms > '2022-09-21 19:14:58.651') AND (event_timestamp_in_ms < '2022-09-22 11:28:18.651')"

 