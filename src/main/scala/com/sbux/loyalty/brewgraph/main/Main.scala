package com.sbux.loyalty.brewgraph.main
import org.apache.spark.SparkConf
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.SparkContext
import scalaj.http.Http
import scala.collection.mutable.ListBuffer
import java.util.Calendar
object Main {
  val output :  ListBuffer[(String,Double,Double,Double)] = ListBuffer()
  def main(args : Array[String]) {
//    var s = Http("http://host.openapi.starbucks.com/product/v1/products/us/11006200").param("locale", "en-us").param("format","json").asString
//    println(s)
    val conf = new SparkConf().setMaster("local").setAppName("BrewGraph");
    val sc = new SparkContext(conf);
    
//    val mopProducts = sc.textFile("/aveettil/mopcatalog/MOP_products")
//    val mapSkuToDescrMapping = mopProducts.map{ line => {val split=line.split(","); return (split(1),split(0))}}
//    val brodcastedMap = sc.broadcast(mapSkuToDescrMapping)
    
    
    var textFile = sc.textFile("/projects/mop/input_midday.csv")
    var outFile = "/projects/sim/output-"+Calendar.getInstance().getTime()+".txt"
    if(args.length > 0) {
      textFile = sc.textFile(args(0))
      outFile = args(1)
    }
    
   // val textFile = sc.textFile("hdfs://bda01-clu-ns/user/aveettil")
    
   // println(y1(0))
    val counts = textFile.map(line => pairSplit(line)).groupByKey()
   // textFile.map { line => line.split(",")}.map(word=>
   // val y = counts.collect()
    
    
    
    //val y_grouped = y.groupBy({case (key, value) =>key})
    
    counts foreach {case (key, value) =>computeJaccard(key,value.toArray)}
    
    //output foreach ( pair => println(pair._1+"-->"+pair._2))
    sc.parallelize(output.toList).map(x=> x._1+","+x._2+","+x._3+","+x._4).saveAsTextFile(outFile)
    
  }
  def pairSplit(x:String):(String,String) = {
    val splitLine = x.split(",")
   
    return (splitLine(0)+","+splitLine(1)+","+splitLine(5), x)
    //return ("1","2")
  }
  def computeJaccard(key:String,values:Array[String])   = {
    println(key)
    
  // val itemVectors = values.groupBy ({ x => x.split(",")(1) }).mapValues{value => value.map{x=>x.split(",")(0).concat(",").concat(x.split(",")(2))}}
     val itemVectors = values.groupBy ({ x => x.split(",")(3) }).mapValues{value => value.map{x=>x.split(",")(2)}}
  //  val output:ListBuffer[(String,Double)] = ListBuffer()
   val itemVectors_new = itemVectors.foreach {case (key1,value1)=> {
      itemVectors.map {case (key2,value2)=> {
           // 
        val score:(String,Double,Double,Double) = getOutputTuple(key,key1,key2,value1,value2)
        output += score
        //val output1:Map[String,Double] = Map()
        //output1 + ("2"->2.0)
        //print (output1.toString())
      }
      }
    
      
//      itemVectors_new.foreach {case _:Map[String,Double] => {
//        println(key)
//      }
//      }
    // println(key .concat("-----").concat(value.mkString(",")))
     } 
   }
   var limit = itemVectors.size
    
   // compute pairwise similarity
//   for (i<- 1 to limit) {
//     for (j <- i+1 to limit -1) {
//       
//     }
//   }
   // compute pairwise jaccard
   //sc.parallelize(output).saveAsTextFile("/projects/sim/output1.txt");
    
  }
  
  def getOutputTuple (locationkey:String,key1:String,key2:String,value1:Array[String],value2:Array[String]) : (String,Double,Double,Double) = {
         if (key1 != key2) {
           val jaccardSimilarity = getJaccard(value1,value2)
           //return jaccardSimilarity
           return (locationkey.concat(",").concat(key1).concat(",").concat(key2),jaccardSimilarity._3,jaccardSimilarity._1,jaccardSimilarity._2)
          // println("%s %s %f %f %f".format(key1,key2,jaccardSimilarity._3,jaccardSimilarity._1,jaccardSimilarity._2))
        } else {
          return (locationkey.concat(",").concat(key1).concat(",").concat(key2),1.0,value1.length,value1.length)
        }
  }
  
   def getJaccard(row1:Array[String], row2:Array[String]) : (Double,Double,Double) = {
       var row1_set = row1.toSet
       var row2_set = row2.toSet
       var intersection = row1_set.intersect(row2_set).size.toFloat
       
       var union = row1_set.union(row2_set).size.toFloat
       return ( intersection,union,intersection/union)
    
  }
 
}