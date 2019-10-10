# Test commands
## Read files from s3
inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz"

## Read files from local
inputPath = "./dataset/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz"

## Spark submit local
spark-submit --driver-memory 12g --class com.rmit.spark.BigDataProcessing ./target/scala-2.11/bigdataprocas2-assembly-0.1.jar local ./dataset/aucities.csv ./dataset/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz kwinana richmond

## Spark submit EMR
spark-submit --master yarn --executor-memory 4g --class com.rmit.spark.BigDataProcessing ./bigdataprocas2-assembly-0.1.jar emr ./aucities.csv s3://commoncrawl/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz kwinana richmond

# AU Cities header
city,lat,lng,country,iso2,admin,capital,population,population_proper

