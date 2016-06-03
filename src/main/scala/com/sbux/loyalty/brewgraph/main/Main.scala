package com.sbux.loyalty.brewgraph.main

import java.util.Calendar
import scala.collection.mutable.ListBuffer
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.time.LocalDate
import scala.math.pow
import java.util.Date

/*
 * computes product to product associations.
 * command line arguments 
 * in - input file which contains country,region,timeofday,guid/txid,item,qty
 * out - output file location in hdfs/s3
 * c - index of position of country column in the input raw. indexes starts from 0.
 * r - index of position of region column in the input raw. indexes starts from 0.
 * t - index of position of timeofday column in the input raw. indexes starts from 0.
 * i - index of position of item column in the input raw. indexes starts from 0.
 * u - index of position of user/txid column in the input raw. indexes starts from 0.
 * 
 * example command line run in spark
 * nohup spark-submit --class com.sbux.loyalty.brewgraph.main.Main 
 * --master yarn-client --deploy-mode client --driver-memory 20g --executor-memory 15g 
 * --num-executors 6 --executor-cores 4 --conf spark.kryoserializer.buffer.max=512m 
 * --conf spark.driver.maxResultSize=10g /home/aveettil/brewgraph-1.0.jar 
 * -in hdfs:///user/aveettil/mop-train-pos-basket -out hdfs:///user/aveettil/brewgraph-out-basket -u 0 -c 1 -r 2 -i 4 -t 6 > out.txt &
 */
object Main {
  var indexOfCountry = 0
  var indexOfRegion = 1
  var indexOfTimeOfDay = 5
  var indexOfItem = 3
  var indexOfUser = 2
  var inputFileName = "/projects/mop/mop-train-pos-basket-short.csv"
  var outFileName = "/projects/sim/output-" + Calendar.getInstance().getTime() + ".txt"
  var alpha:Double =0.0
  var indexOfTxDay = 6
  var indexOfItemCount = 7
  var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  var delimiter = ","
  var endDay:Date = null
  val output: ListBuffer[(String, Double, Double, Double)] = ListBuffer()
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("BrewGraph"); // master should be set from command line with --master
    val sc = new SparkContext(conf);

    // updates the class variables (indexOfCountry,indexOfCountry, etc) if a value is specified as command arguments
    getOptions(args)
    var inputFile = sc.textFile(inputFileName);

    val counts = inputFile.map(line => pairSplit(line, indexOfCountry, indexOfRegion, indexOfTimeOfDay)).groupByKey()

   // counts foreach { case (key, value) => computeJaccard(key, value.toArray, indexOfItem, indexOfUser) }
    counts foreach { case (key, value) => computeJaccardWithTimeDecay (key, value.toArray, indexOfItem, indexOfUser,indexOfTxDay,indexOfItemCount) }

    sc.parallelize(output.toList).map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4).saveAsTextFile(outFileName)

  }
  def help(options: Options) = {

    var formater = new HelpFormatter();

    formater.printHelp("Main", options);

    System.exit(0);
  }

  def pairSplit(x: String, indexOfCountry: Integer, indexofregion: Integer, indexOfTimeOfDay: Integer): (String, String) = {
    val splitLine = x.split(delimiter)
    val country = splitLine(indexOfCountry);
    val region = splitLine(indexofregion);
    val timeofday = splitLine(indexOfTimeOfDay);
    return (country + delimiter + region + delimiter + timeofday, x)
    //return ("1","2")
  }
  
  /*
   * computes Jaccard coefficient between two vectors and also appends it to a list
   */
  def computeJaccard(key: String, values: Array[String], indexofItem: Integer, indexOfcolumnvalue: Integer) = {
    println(key)

    val itemVectors = values.groupBy({ x => x.split(delimiter)(indexofItem) }).mapValues { value => value.map { x => x.split(delimiter)(indexOfcolumnvalue) } }

    val itemVectors_new = itemVectors.foreach {
      case (key1, value1) => {
        itemVectors.map {
          case (key2, value2) => {
            // 
            val score: (String, Double, Double, Double) = getOutputTuple(key, key1, key2, value1, value2)
            output += score
          }
        }

      }
    }
    var limit = itemVectors.size

  }
  
  /*
   * computes Jaccard coefficient between two vectors and also appends it to a list
   */
  def computeJaccardWithTimeDecay(key: String, values: Array[String], indexofItem: Integer, indexOfcolumnvalue: Integer,indexTxDate: Integer,indexCount:Integer) = {
    println(key)

    val itemVectors = values.groupBy({ x => x.split(delimiter)(indexofItem) }).mapValues { value => value.map { x => appendTxDay(x, indexOfcolumnvalue, indexTxDate, indexCount) } }

    val itemVectors_new = itemVectors.foreach {
      case (key1, value1) => {
        itemVectors.map {
          case (key2, value2) => {
            // 
            val score: (String, Double, Double, Double) = getOutputTupleWithTimeDecay(key, key1, key2, value1, value2)
            output += score
          }
        }

      }
    }
    var limit = itemVectors.size

  }
  
  def appendTxDay(x:String,indexOfcolumnvalue: Integer,indexTxDate: Integer,indexCount:Integer):(String,Int) =  {
        var split = x.split(delimiter)
        var retValue = (split(indexOfcolumnvalue)+";"+split(indexTxDate),split(indexCount).toInt)
        return retValue;
  }

  /**
   * creates pairwise similarity
   */
  def getOutputTuple(locationkey: String, key1: String, key2: String, value1: Array[String], value2: Array[String]): (String, Double, Double, Double) = {
    if (key1 != key2) {

      // you can call other similarity functions instead of jaccard here
      val jaccardSimilarity = getJaccard(value1, value2)
      //return jaccardSimilarity - key item1,item2 value jaccard similarity with counts for union and intersection
      
      // note - Code can be updated to weed out similarities which does not have minimum threshold for intersection and union
      return (locationkey.concat(delimiter).concat(key1).concat(delimiter).concat(key2), jaccardSimilarity._3, jaccardSimilarity._1, jaccardSimilarity._2)

    } else {
          return (locationkey.concat(delimiter).concat(key1).concat(delimiter).concat(key2), 1.0, value1.length, value1.length)
    }
  }
  
   /**
   * creates pairwise similarity
   */
  def getOutputTupleWithTimeDecay(locationkey: String, key1: String, key2: String, value1: Array[(String,Int)], value2: Array[(String,Int)]): (String, Double, Double, Double) = {
    if (key1 != key2) {

      // you can call other similarity functions instead of jaccard here
      val jaccardSimilarity = getJaccardWithTimeDecay(value1, value2,alpha)
      //return jaccardSimilarity - key item1,item2 value jaccard similarity with counts for union and intersection
      
      // note - Code can be updated to weed out similarities which does not have minimum threshold for intersection and union
      return (locationkey.concat(delimiter).concat(key1).concat(delimiter).concat(key2), jaccardSimilarity._3, jaccardSimilarity._1, jaccardSimilarity._2)

    } else {
          return (locationkey.concat(delimiter).concat(key1).concat(delimiter).concat(key2), 1.0, value1.length, value1.length)
    }
  }

  /**
   * computes Jaccard coefficient between two vectors
   * Jaccard's similarity = c/(a+b-c) where c= number of bits common to bothrow1 and row2
     a = number of 1s in row1, b=number of 1s in 
   */
  def getJaccard(row1: Array[String], row2: Array[String]): (Double, Double, Double) = {
    var row1_set = row1.toSet
    var row2_set = row2.toSet
    var intersection = row1_set.intersect(row2_set).size.toFloat

    var union = row1_set.union(row2_set).size.toFloat
    return (intersection, union, intersection / union)

  }
  
  /**
   * computes Jaccard coefficient between two vectors
   * Jaccard's similarity = c/(a+b-c) where c= number of bits common to bothrow1 and row2
     a = number of 1s in row1, b=number of 1s in 
     to add time decay, instead of adding up 1s, add time decay value for computing a,b,c
   */
  def getJaccardWithTimeDecay(row1: Array[(String,Int)], row2: Array[(String,Int)],alpha:Double): (Double, Double, Double) = {
    
    var set = collection.mutable.Set[String]()
    
    var union = 0.0
    row1.foreach( { case(key,value) => {
      set+=key
      var decay = pow(Math.E,-1 * getTimeDeltaDays(key.split(";")(1)) * alpha) // multiply with time alpha
      union+= decay
    }
    })
    var intersection = 0.0
     row2.foreach( { case(key,value) => {
       var decay = pow(Math.E,-1 * getTimeDeltaDays(key.split(";")(1)) * alpha) // multiply with time alpha
        union+= decay
        if (set.contains(key)){
         
          intersection += decay
         
        }
       
    }
    })
    union = union - intersection
    return (intersection, union, intersection / union)
  }
    
    def getTimeDeltaDays(date:String):Long = {
     
      var date1 = dateFormat.parse(date).getTime()
       
      var diffInMillis:Long = endDay.getTime()-date1
      var tu:TimeUnit =TimeUnit.DAYS
      var retValue =  tu.convert(diffInMillis,TimeUnit.MILLISECONDS)
      return retValue
      
    }
  


  def getOptions(args: Array[String]) = {

    val options = new Options();

    
    options.addOption("in", true, "input file");

    options.addOption("out", true, "output file");

    options.addOption("c", true, "index of country");

    options.addOption("r", true, "index of region");

    options.addOption("t", true, "index of time of day");

    options.addOption("i", true, "index of item");

    options.addOption("u", true, "index of user");
    
    options.addOption("alpha", true, "time decay parameter");
    
    options.addOption("dt", true, "index of date of transaction");
    
    options.addOption("n", true, "index of item count");
    
    options.addOption("del", true, "delimiter");
    options.addOption("end", true, "delimiter");

    val parser = new BasicParser();

    val cmd = parser.parse(options, args);

    if (cmd.hasOption("h")) {
      help(options)
    }

    if (cmd.hasOption("in")) {
      inputFileName = cmd.getOptionValue("in")
    }

    val allOptions = cmd.getOptions()

    if (cmd.hasOption("out")) {

      outFileName = cmd.getOptionValue("out")
    }

    if (cmd.hasOption("c")) {

      indexOfCountry = cmd.getOptionValue("c").toInt
    }
    if (cmd.hasOption("r")) {
      indexOfRegion = cmd.getOptionValue("r").toInt
    }
    if (cmd.hasOption("t")) {
      indexOfTimeOfDay = cmd.getOptionValue("t").toInt
    }
    if (cmd.hasOption("i")) {
      indexOfItem = cmd.getOptionValue("i").toInt
    }
    if (cmd.hasOption("u")) {
      indexOfUser = cmd.getOptionValue("u").toInt
    }
    
    if (cmd.hasOption("alpha")) {
      alpha = cmd.getOptionValue("alpha").toDouble
    }
    
    if (cmd.hasOption("dt")) {
      indexOfTxDay = cmd.getOptionValue("dt").toInt
    }
     if (cmd.hasOption("n")) {
      indexOfItemCount = cmd.getOptionValue("n").toInt
    }
    if (cmd.hasOption("del")) {
      delimiter = cmd.getOptionValue("del")
    }
    if (cmd.hasOption("end")) {
      var sEndDay = cmd.getOptionValue("end")
      endDay = dateFormat.parse(sEndDay)
    } else {
      endDay = new Date();
    }
  }

}