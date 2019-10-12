HOW TO COMPILE AND RUN THIS PROJECT

1. PREREQUISITES:
+ An EMR cluster using EC2 instances with at least 16GiB of RAM - preferably m5.xlarge instances
+ The cluster should have 1 master node and at least 2 core nodes
+ Simple Build Tool for Scala (sbt) installed
+ JDK for Java 8 installed
+ Spark 2.3.0 installed

2. COMPILING THE PROJECT
+ Navigating to folder containing the build.sbt file
+ Use the command "sbt assembly" to compile the project and create a jar file
or use the pre-generated jar file in target/scala-2.11/

3. RUN THE JAR FILE LOCALLY
+ Make sure your computer has at least 16GiB of RAM. Then run this command:
spark-submit --driver-memory 12g --class com.rmit.spark.BigDataProcessing ./target/scala-2.11/bigdataprocas2-assembly-0.1.jar local ./dataset/aucities.csv ./dataset/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz kwinana richmond

4. RUN THE JAR FILE ON EMR
+ Upload this jar file to hdfs or S3 where it can be downloaded to the master node
+ Connect to the master node using SSH
+ Using hadoop fs -copyToLocal or aws s3 cp to download the jar file
+ To test a single WET file from Common Crawl S3, use this command:
spark-submit --master yarn --executor-memory 8g --class com.rmit.spark.BigDataProcessing ./bigdataprocas2-assembly-0.1.jar emr ./aucities.csv s3://commoncrawl/crawl-data/CC-MAIN-2019-39/segments/1568514570740.10/wet/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz kwinana richmond
+ To test two or more WET files, create a S3 bucket and add two or more WET file from Common Crawl S3 into it using aws s3 cp
as using the entire Common Crawl S3 dataset could exceed the limit of current cluster resource. Then run this command:
spark-submit --master yarn --executor-memory 8g --class com.rmit.spark.BigDataProcessing ./bigdataprocas2-assembly-0.1.jar emr ./aucities.csv s3://<your-s3-bucket> kwinana richmond

5. PARAMETERS
Command: spark-submit --master yarn --executor-memory 8g --class <class-name> <jar-file> <run-mode> <city-list> <source-files> <bfs-start-city> <bfs-end-city>
+ class-name: class name of the Scala program
+ jar-file: path to jar file
+ run-mode: local or emr (used to define Spark Context)
+ city-list: path to file contains city list
+ source-file: path to WET files
+ bfs-start-city: name of start city
+ bfs-end-city: name of target city for BFS search