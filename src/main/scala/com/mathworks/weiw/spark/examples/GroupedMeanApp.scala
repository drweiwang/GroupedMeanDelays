/**
 * Created by weiw on 7/28/14.
 */

package com.mathworks.weiw.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark._

object GroupedMeanApp {

  def main(args: Array[String]) {
    val filePath  = if (args.length > 0) args(0) else "/sandbox/bigdata/datasets/airline/2008.csv"
    val conf = new SparkConf().setAppName("Spark Example: Grouped Mean")
    val sc = new SparkContext(conf)
    val dataSet = sc.textFile(filePath).filter(line => line.split(",")(14).matches("""\d+"""))
    val delaysRDD = dataSet.map(getDelays)
    val groupMean = delaysRDD.reduceByKey((a:(Double,Double),b:(Double,Double)) => (a._1 +b._1, a._2 + b._2)).map(x =>(x._1, x._2._1/x._2._2)).collect

     println(groupMean.mkString("\n"))
  }

  def getDelays(line:String ): (String, (Double, Double)) ={
    val fields = line.split(",")
    val airline = fields(8)
    val delay = fields(14)
    (airline, (delay.toDouble, 1.0))

  }
}
