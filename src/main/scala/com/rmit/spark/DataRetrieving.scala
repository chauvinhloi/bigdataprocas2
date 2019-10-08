package com.rmit.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.lang.String

object DataRetrieving {
  def getDomain(content: String): (String, String) = {
    val uriPattern = "WARC-Target-URI: http(|s)://([a-z0-9.]*)/".r
    val domain = uriPattern.findFirstIn(content).getOrElse("DomainNotFound")
    if (domain == "DomainNotFound") {
      ("DomainNotFound", content)
    } else {
      (domain.split("\\/")(2), content)
    }
  }

  /* main function where Spark job starts */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Config to be used on EMR
    /*
    val conf = new SparkConf()
    conf.setAppName("Data Retrieving")
    val sc = new SparkContext(conf)
    */

    // Config to be used on local machine
    val sc = new SparkContext("local[*]", "Data Retrieving")

    // Taking input from arguments
    val inputPath = args(0)

    println(s"\nLoading data from $inputPath...")

    // Reading as a single file and reserve the order of content by not splitting lines and sends across cluster
    val docs = sc.wholeTextFiles(inputPath).map({ case(_, content) => content }).flatMap(chunks => chunks.split("WARC/1.0"))

    // Number of records in the dataset
    val numberOfDocs = docs.count()

    // Print out number of documents
    println(s"There are $numberOfDocs documents in this dataset")

    // Create new RDD as ( Domain, Content )
    val docsWithDomain = docs.map(getDomain)

    // Only keep .au domain
    val dotAUPages = docsWithDomain.filter( x => x._1.contains(".au")  )

    // Count number of .au documents
    val numberOfAUDocs = dotAUPages.count()

    // Print out number of .au documents
    println(s"There are $numberOfAUDocs .au documents in this dataset")
  }
}
