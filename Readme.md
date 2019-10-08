# Test commands
## Read files from s3
inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/"

## Read files from local
inputPath = "./dataset/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz"

## Spark submit local
spark-submit --driver-memory 12g --class com.rmit.spark.DataRetrieving ./target/scala-2.11/bigdataprocas2-assembly-0.1.jar ./dataset/