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

  var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val output: ListBuffer[(String, Double, Double, Double)] = ListBuffer()
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("BrewGraph")//.setMaster("local"); // master should be set from command line with --master
   
    val sc = new SparkContext(conf);

    // updates the class variables (indexOfCountry,indexOfCountry, etc) if a value is specified as command arguments
    val inputBean:InputBean = getOptions(args)
    var inputFile = sc.textFile(inputBean.inputFileName);
println(inputBean.toTextString());
    val counts = inputFile.map(line => pairSplit(line, inputBean)).groupByKey()

   // counts foreach { case (key, value) => computeJaccard(key, value.toArray, indexOfItem, indexOfUser) }
    counts foreach { case (key, value) => computeJaccardWithTimeDecay (key, value.toArray, inputBean) }

    sc.parallelize(output.toList).map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4).saveAsTextFile(inputBean.outFileName)

  }
  def help(options: Options) = {

    var formater = new HelpFormatter();

    formater.printHelp("Main", options);

    System.exit(0);
  }

  def pairSplit(x: String, inputBean:InputBean): (String, String) = {
    val splitLine = x.split(inputBean.delimiter)
    val country = splitLine(inputBean.indexOfCountry);
    val region = splitLine(inputBean.indexOfRegion);
    val timeofday = splitLine(inputBean.indexOfTimeOfDay);
    //println(x)
    return (country + inputBean.delimiter + region + inputBean.delimiter + timeofday, x)
    //return ("1","2")
  }
  
  /*
   * computes Jaccard coefficient between two vectors and also appends it to a list
   */
  def computeJaccard(key: String, values: Array[String], inputBean:InputBean) = {
    println(key)

    val itemVectors = values.groupBy({ x => x.split(inputBean.delimiter)(inputBean.indexOfItem) }).mapValues { value => value.map { x => x.split(inputBean.delimiter)(inputBean.indexOfUser) } }

    val itemVectors_new = itemVectors.foreach {
      case (key1, value1) => {
        itemVectors.map {
          case (key2, value2) => {
            // 
            val score: (String, Double, Double, Double) = getOutputTuple(key, key1, key2, value1, value2,inputBean)
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
  def computeJaccardWithTimeDecay(key: String, values: Array[String], inputBean:InputBean) = {
    println(key)
    println(inputBean.toTextString());
    val itemVectors = values.groupBy({ x => x.split(inputBean.delimiter)(inputBean.indexOfItem) }).mapValues { value => value.map { x => appendTxDay(x, inputBean ) } }

    val itemVectors_new = itemVectors.foreach {
      case (key1, value1) => {
        itemVectors.map {
          case (key2, value2) => {
            // 
            val score: (String, Double, Double, Double) = getOutputTupleWithTimeDecay(key, key1, key2, value1, value2,inputBean)
            output += score
          }
        }

      }
    }
    var limit = itemVectors.size

  }
  
  def appendTxDay(x:String,inputBean:InputBean):(String,Int) =  {
        var split = x.split(inputBean.delimiter)
        var retValue = (split(inputBean.indexOfUser)+";"+split(inputBean.indexOfTxDay),split(inputBean.indexOfItemCount).toInt)
        return retValue;
  }

  /**
   * creates pairwise similarity
   */
  def getOutputTuple(locationkey: String, key1: String, key2: String, value1: Array[String], value2: Array[String],inputBean:InputBean): (String, Double, Double, Double) = {
     var delimiter = inputBean.delimiter
    if (key1 != key2) {

      // you can call other similarity functions instead of jaccard here
      val jaccardSimilarity = getJaccard(value1, value2)
      //return jaccardSimilarity - key item1,item2 value jaccard similarity with counts for union and intersection
     
      // note - Code can be updated to weed out similarities which does not have minimum threshold for intersection and union
      return (locationkey.concat( delimiter).concat(key1).concat(delimiter).concat(key2), jaccardSimilarity._3, jaccardSimilarity._1, jaccardSimilarity._2)

    } else {
          return (locationkey.concat(delimiter).concat(key1).concat(delimiter).concat(key2), 1.0, value1.length, value1.length)
    }
  }
  
   /**
   * creates pairwise similarity
   */
  def getOutputTupleWithTimeDecay(locationkey: String, key1: String, key2: String, value1: Array[(String,Int)], value2: Array[(String,Int)],inputBean:InputBean): (String, Double, Double, Double) = {
     var delimiter = inputBean.delimiter
    if (key1 != key2) {

      // you can call other similarity functions instead of jaccard here
      val jaccardSimilarity = getJaccardWithTimeDecay(value1, value2,inputBean.alpha,inputBean.endDay)
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
  def getJaccardWithTimeDecay(row1: Array[(String,Int)], row2: Array[(String,Int)],alpha:Double,endDay:Date): (Double, Double, Double) = {
    
    var set = collection.mutable.Set[String]()
    
    var union = 0.0
    row1.foreach( { case(key,value) => {
     // println("key is"+key)
      set+=key
      var decay = pow(Math.E,-1 * getTimeDeltaDays(key.split(";")(1),endDay) * alpha) // multiply with time alpha
      union+= decay
    }
    })
    var intersection = 0.0
     row2.foreach( { case(key,value) => {
       //println("key is"+key)
       var decay = pow(Math.E,-1 * getTimeDeltaDays(key.split(";")(1),endDay) * alpha) // multiply with time alpha
        union+= decay
        if (set.contains(key)){
         
          intersection += decay
         
        }
       
    }
    })
    union = union - intersection
    return (intersection, union, intersection / union)
  }
    
    def getTimeDeltaDays(date:String,endDay:Date):Long = {
     if(date.equals(""))
       return 0L; // edge case when no date is present in the input data
     var date1 = System.currentTimeMillis()
     try {
       date1 = dateFormat.parse(date).getTime()
     } catch{
       case e:Exception => {}//e.printStackTrace()}
       return 0
     }
     // println(date1+"     "+endDay)
      var diffInMillis:Long = endDay.getTime()-date1
      var tu:TimeUnit =TimeUnit.DAYS
      var retValue =  tu.convert(diffInMillis,TimeUnit.MILLISECONDS)
      return retValue
      
      
    }
  


  def getOptions(args: Array[String]):InputBean = {

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

    var inputBean = new InputBean()
    if (cmd.hasOption("h")) {
      help(options)
    }

    if (cmd.hasOption("in")) {
      inputBean.inputFileName = cmd.getOptionValue("in")
    }

    val allOptions = cmd.getOptions()

    if (cmd.hasOption("out")) {

      inputBean.outFileName = cmd.getOptionValue("out")
    }

    if (cmd.hasOption("c")) {

      inputBean.indexOfCountry = cmd.getOptionValue("c").toInt
    }
    if (cmd.hasOption("r")) {
      inputBean.indexOfRegion = cmd.getOptionValue("r").toInt
    }
    if (cmd.hasOption("t")) {
      inputBean.indexOfTimeOfDay = cmd.getOptionValue("t").toInt
    }
    if (cmd.hasOption("i")) {
      inputBean.indexOfItem = cmd.getOptionValue("i").toInt
    }
    if (cmd.hasOption("u")) {
      inputBean.indexOfUser = cmd.getOptionValue("u").toInt
    }
    
    if (cmd.hasOption("alpha")) {
      inputBean.alpha = cmd.getOptionValue("alpha").toDouble
    }
    
    if (cmd.hasOption("dt")) {
      inputBean.indexOfTxDay = cmd.getOptionValue("dt").toInt
    }
     if (cmd.hasOption("n")) {
      inputBean.indexOfItemCount = cmd.getOptionValue("n").toInt
    }
    if (cmd.hasOption("del")) {
      inputBean.delimiter = cmd.getOptionValue("del")
    }
    if (cmd.hasOption("end")) {
      var sEndDay = cmd.getOptionValue("end")
      // println(sEndDay);
      inputBean.endDay = dateFormat.parse(sEndDay)
     // println(inputBean.endDay.toGMTString());
    } else {
      inputBean.endDay = new Date();
    }
    return inputBean
  }

}