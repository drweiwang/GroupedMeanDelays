package com.mathworks.weiw.spark.examples

/**
 * Summary statistics by group. Using airline on-time performance dataset.
 * Created by Wei Wang on 7/28/14.
 */

import org.apache.spark.SparkContext._
import org.apache.spark._

object GroupedStatsApp {

  def main(args: Array[String]) {
    val t0 = System.nanoTime()

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
    val gSSQ = dataSet.reduceByKey((x,y) => math.pow(x,2)+math.pow(y,2)).join(gCount).map(mapFun)
    val gVar = gMean.join(gSSQ).map(x => (x._1, x._2._2-x._2._1))

    println("Mean by group: \n" + gMean.collect().mkString("\n"))
    println("Variance by group:\n" + gVar.collect().mkString("\n"))

    val t1 = System.nanoTime()
    println("Elapsed time: "+ (t1-t0)*1e9 +"seconds")
  }
}
