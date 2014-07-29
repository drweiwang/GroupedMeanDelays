package com.mathworks.weiw.spark.examples

/**
 * Summary statistics by group. Using airline on-time performance dataset.
 * Created by Wei Wang on 7/28/14.
 */

import org.apache.spark.SparkContext._
import org.apache.spark._

object GroupedStatsApp {

  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()

    // Parse input arguments
    val grpIdx =  if (args.length > 0) args(0).toInt else 3
    val filePath  = if (args.length > 1) args(1) else "/sandbox/bigdata/datasets/airline/2008.csv"

    // Set SparkConf configuration object and SparkContext object
    val conf = new SparkConf().setAppName("Spark Example: Grouped Mean")
    val sc = new SparkContext(conf)

    // Remove missing values
    val dataSet = sc.textFile(filePath).filter(line => line.split(",")(14).matches("""[-+]?[0-9]*\.?[0-9]+""")).map(line => {val f = line.split(","); (f(grpIdx), f(14).toDouble)})

    // Map & Reduce functions:
    val mapFun = (x:(String, (Double, Double))) =>(x._1, x._2._2/x._2._1)
    val reduceFun = (x:Double, y:Double)=> x+y

    val gCount = dataSet.map(p => (p._1,1.0)).reduceByKey(reduceFun)
    val gSum = dataSet.reduceByKey(reduceFun)
    val gMean = gCount.join(gSum).map(mapFun)
    val gMeanHashMap = gMean.collectAsMap()
    println("\nMean by group: \n" + gMean.collect().mkString("\n") + "\n")

    val gSSQ = dataSet.map(x=>(x._1, math.pow(x._2 - gMeanHashMap.get(x._1).get,2))).reduceByKey(reduceFun)
    val gVar = gCount.join(gSSQ).map(mapFun)

    println("\nVariance by group:\n" + gVar.collect().mkString("\n") + "\n")

    val t1 = System.currentTimeMillis()
    println("Elapsed time: "+ (t1-t0).toDouble*1e-3 +"seconds")
  }
}
