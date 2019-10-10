package com.rmit.spark

import org.apache.spark._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._

import scala.io.Source
import java.nio.charset.CodingErrorAction
import java.io._

import scala.io.Codec
import org.apache.spark.rdd._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.hashing.MurmurHash3.stringHash
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object BigDataProcessing {
  // A Node contain city and an array of connected cities
  type Node = (String, Array[String])

  // BFSData contains an array of city connections, the distance, and status.
  type BFSData = (Array[String], Int, String)

  // A BFSNode has a city name and the BFSData associated with it.
  type BFSNode = (String, BFSData)

  def getDomain(content: String): (String, String) = {
    val uriPattern = "WARC-Target-URI: http(|s)://([a-z0-9.]*)/".r
    val domain = uriPattern.findFirstIn(content).getOrElse("DomainNotFound")
    if (domain == "DomainNotFound") {
      ("DomainNotFound", content)
    } else {
      (domain.split("\\/")(2), content)
    }
  }

  def isIn(word: String, wordsArray: Array[String]): Boolean = {
    // Check if word is in the provided list
    wordsArray.contains(word)
  }

  def citiesRelation(occurrences: Array[String]): Array[Node] = {
    var result: ArrayBuffer[Node] = ArrayBuffer()
    for (city <- occurrences) {
      val node = (city, occurrences)
      result += node
    }
    result.toArray
  }

  def loadCities(citiesPath: String): Array[String] = {
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var cities: ArrayBuffer[String] = ArrayBuffer()

    val bufferedReader = Source.fromFile(citiesPath)
    val lines = bufferedReader.getLines()
    for (line <- lines) {
      val fields = line.split(",")
      if (fields.length > 1) {
        cities += fields(0).trim().toLowerCase()
      }
    }

    bufferedReader.close()
    cities.toArray
  }

  def loadGraph(graphPath: String, startCity: String): Array[BFSNode] = {
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var nodes: ArrayBuffer[BFSNode] = ArrayBuffer()

    val bufferedReader = Source.fromFile(graphPath)
    val lines = bufferedReader.getLines()
    for (line <- lines) {
      val bfsNode = convertToBFS(line, startCity)
      nodes += bfsNode
    }
    nodes.toArray
  }


  def sortPair(pair: (String, String)): (String, String) = {
    if (pair._2 < pair._1) {
      (pair._2, pair._1)
    } else {
      (pair._1, pair._2)
    }
  }

  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(line: String, startCity: String): BFSNode = {

    // Split up the line into fields
    val fields = line.split("\\s+")

    // Extract this city name from the first field
    val cityName = fields(0)

    // Extract subsequent city name into the connections array
    var connections: ArrayBuffer[String] = ArrayBuffer()
    for (connection <- 1 until fields.length) {
      connections += fields(connection)
    }

    // Default distance and state is 9999 (infinity) and UNEXPLORED
    var status: String = "UNEXPLORED"
    var distance: Int = 9999

    // Unless this is the city we're starting from
    if (cityName == startCity) {
      status = "TO_BE_EXPLORED"
      distance = 0
    }

    (cityName, (connections.toArray, distance, status))
  }

  /** Expands a BFSNode into this node and its children */
  def bfsMap(node: BFSNode, targetCity: String, hitCounter: Accumulator[Int]): Array[BFSNode] = {
    // Extract data from the BFSNode
    val cityName: String = node._1
    val data: BFSData = node._2

    val connections: Array[String] = data._1
    val distance: Int = data._2
    var state: String = data._3

    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our new RDD
    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    // TO_BE_EXPLORED nodes are flagged for expansion, and create new
    // TO_BE_EXPLORED nodes for each connection
    if (state == "TO_BE_EXPLORED") {
      for (connection <- connections) {
        val newCityName = connection
        val newDistance = distance + 1
        val newState = "TO_BE_EXPLORED"

        // Have we stumbled across the character we're looking for?
        // If so increment our accumulator so the driver script knows.
        if (targetCity == connection) {
          hitCounter.add(1)
        }

        // Create our new TO_BE_EXPLORED node for this connection and add it to the results
        val newEntry: BFSNode = (newCityName, (Array(), newDistance, newState))
        results += newEntry
      }
      println()

      // Color this node as EXPLORED, indicating it has been processed already.
      state = "EXPLORED"
    }

    // Add the original node back in, so its connections can get merged with
    // the TO_BE_EXPLORED nodes in the reducer.
    val thisEntry: BFSNode = (cityName, (connections, distance, state))
    results += thisEntry

    results.toArray
  }

  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {

    // Extract data that we are combining
    val edges1: Array[String] = data1._1
    val edges2: Array[String] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val state1: String = data1._3
    val state2: String = data2._3

    // Default node values
    var distance: Int = 9999
    var state: String = "UNEXPLORED"
    var edges: ArrayBuffer[String] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (state1 == "UNEXPLORED" && (state2 == "TO_BE_EXPLORED" || state2 == "EXPLORED")) {
      state = state2
    }
    if (state1 == "TO_BE_EXPLORED" && state2 == "EXPLORED") {
      state = state2
    }
    if (state2 == "UNEXPLORED" && (state1 == "TO_BE_EXPLORED" || state1 == "EXPLORED")) {
      state = state1
    }
    if (state2 == "TO_BE_EXPLORED" && state1 == "EXPLORED") {
      state = state1
    }

    (edges.toArray, distance, state)
  }

  /* main function where Spark job starts */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Taking input from arguments
    val platform = args(0)
    val citiesPath = args(1)
    val inputPath = args(2)
    val startCity = args(3).toLowerCase()
    val targetCity = args(4).toLowerCase()

    // Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("Big Data Processing")
    if (platform == "local") {
      // Config to be used on local machine
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    // Broadcasting target city to all the mappers
    val targetCityBroadcasted = sc.broadcast(targetCity)


    //**********************************************************************************/
    /* GENERATING GRAPH DATA                                                           */
    //**********************************************************************************/

    println(s"\nLoading data from $inputPath...")

    // Reading as a single file and reserve the order of content by not splitting lines and sends across cluster
    val docs = sc.wholeTextFiles(inputPath).map({ case (_, content) => content }).flatMap(chunks => chunks.split("WARC/1.0"))

    // Number of records in the dataset
    val numberOfDocs = docs.count()

    // Print out number of documents
    println(s"There are $numberOfDocs documents in this dataset")

    // Create new RDD as ( Domain, Content )
    val docsWithDomain = docs.map(getDomain)

    // Only keep .au domain
    val dotAUPages = docsWithDomain.filter(x => x._1.contains(".au"))

    // Count number of .au documents
    val numberOfAUDocs = dotAUPages.count()

    // Print out number of .au documents
    println(s"There are $numberOfAUDocs .au documents in this dataset")

    // Reading list of cities in Australia
    println(s"Loading cities list from $citiesPath")
    val cities = loadCities(citiesPath)

    // Split using a regular expression that extracts words and generate words RDD with key as document hash value and lower-cased word
    // Output: (hashKey, word)
    println("Preparing graph data...")
    val wordsWithKey = dotAUPages.map(x => (stringHash(x._2), x._2.split("\\W+"))).flatMapValues(x => x)

    // Only keep citi names
    // Output: (hashKey, city)
    val citiesNamesOnly = wordsWithKey.map(x => (x._1, x._2.toLowerCase())).filter(y => isIn(y._2, cities))

    // Cities occurrence per document
    val citiesOccurrencePerDoc = citiesNamesOnly.groupByKey()

    // Keep only distinct occurrences in one document
    val citiesOccurrenceDistinct = citiesOccurrencePerDoc.mapValues(_.toList).map(x => x._2.distinct)

    // Create graph data
    val graph = citiesOccurrenceDistinct.flatMap(x => citiesRelation(x.toArray))

    // Create edge + weight data
    val edgeWeight = graph.flatMapValues(x => x).map(y => (sortPair(y), 1)).filter(z => z._1._1 != z._1._2).countByKey()

    // Write edge data to disk (for visualisation)
    println("Writing edge data to current working directory")
    val edfile = new File("./edgedata.csv")
    val edbw = new BufferedWriter(new FileWriter(edfile))
    for (e <- edgeWeight) {
      val node1 = e._1._1
      val node2 = e._1._2
      val weight = e._2
      edbw.write(s"$node1,$node2,$weight\n")
    }
    edbw.close()

    // Join graph data
    val graphJoin = graph.flatMapValues(x => x).map(y => sortPair(y)).filter(z => z._1 != z._2).groupByKey().mapValues(_.toList).map(w => (w._1, w._2.distinct))

    // Writing graph data
    println("Writing graph data to current working directory")
    val grfile = new File("./graphdata.txt")
    val grbw = new BufferedWriter(new FileWriter(grfile))
    for (g <- graphJoin.collect()) {
      val node = g._1
      val data = g._2.mkString(" ")
      grbw.write(s"$node $data\n")
    }
    grbw.close()

    println()


    //**********************************************************************************/
    /* BFS ALGORITHM                                                                   */
    //**********************************************************************************/

    println(s"Finding degree of separation from ${startCity.toUpperCase} to ${targetCity.toUpperCase}")

    // Our accumulator, used to signal when we find the target
    // city in our BFS traversal.

    var iterationRdd = sc.makeRDD(loadGraph("./graphdata.txt", startCity))

    val hitCounter = sc.accumulator(0)

    var iteration: Int = 0

    breakable {
      while (iteration <= 20) {
        println("Running BFS Iteration " + iteration)

        // Create new vertices as needed to darken or reduce distances in the
        // reduce stage. If we encounter the node we're looking for as a TO_BE_EXPLORED
        // node, increment our accumulator to signal that we're done.
        val mapped = iterationRdd.flatMap(x => bfsMap(x, targetCityBroadcasted.value, hitCounter))
        println(s"${mapped.count()} values processed")

        val hitCount = hitCounter.value
        if (hitCount > 0) {
          println(s"Hit the target city! From $hitCount different direction(s).")
          println(s"${targetCity.toUpperCase} is $iteration node(s) away from ${startCity.toUpperCase}.")
          break()
        }

        // Reducer combines data for each city name, preserving the maximum state
        // and shortest path.
        iterationRdd = mapped.reduceByKey(bfsReduce)
        iteration += 1

      }
    }
  }
}
