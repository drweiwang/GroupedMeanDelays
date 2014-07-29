package com.mathworks.weiw.spark.examples

/**
 * Compute mean arrival delays grouped by a grouping variable.
 * Created by wei wang on 7/28/14.
 */

import org.apache.spark.SparkContext._
import org.apache.spark._

object GroupedMeanApp {

  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()

    val grpIdx =  if (args.length > 0) args(0).toInt else 3
    val filePath  = if (args.length > 1) args(1) else "/sandbox/bigdata/datasets/airline/2008.csv"

    val conf = new SparkConf().setAppName("Spark Example: Grouped Mean")
    val sc = new SparkContext(conf)
    val dataSet = sc.textFile(filePath).filter(line => line.split(",")(14).matches("""[-+]?[0-9]*\.?[0-9]+"""))
    val delaysRDD = dataSet.map((line:String) => {val f=line.split(","); (f(grpIdx), (f(14).toDouble, 1.0))})
    val groupMean = delaysRDD.reduceByKey((a: (Double, Double), b: (Double, Double)) => (a._1 + b._1, a._2 + b._2)).map(x => (x._1, x._2._1 / x._2._2)).collect()

    val t1 = System.currentTimeMillis()
    println("\n\n" + groupMean.mkString("\n"))

    println("\nElapsed time: "+ (t1-t0)*1.0e3 +" seconds")
  }
}
