package com.mathworks.weiw.spark.examples

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * This app performs the logistic regression on the airline datasets.
 * Created by wei wang on 7/30/14.
 */
object LogitApp {

  def main (args:Array[String]){
    val t0 = System.currentTimeMillis()

    // Parse input arguments
    val ITERATION =  if (args.length > 0) args(0).toInt else 5
    val filePath  = if (args.length > 1) args(1) else "/sandbox/bigdata/datasets/airline/2008.csv"

    // Set SparkConf configuration object and SparkContext object
    val conf = new SparkConf().setAppName("Spark Example: Logistic Regression")
    val sc = new SparkContext(conf)

    // Remove missing values
    val points = sc.textFile(filePath)
                    .filter(line => {val f = line.split(","); isNumeric(f(14)) && isNumeric(f(18))})
                    .map(line => {val f = line.split(","); val y = if (f(14).toDouble>20) 1.0 else -1.0; (y, f(18).toDouble/1000)})
                    .cache()
    val D = 2
    val rng = new Random()
    var w = (rng.nextDouble(), rng.nextDouble()) //current separating plane

    for (i<-1 to ITERATION) {
      val gradient = points.map(p => {
        val b = 1 / (1 + math.exp(-1 * p._1 * (w._1 + w._2 * p._2))) - 1
        (b * p._1, b * p._1*p._2)
      }).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      val newW = (w._1 - gradient._1, w._2 - gradient._2)
      w = newW
      println("Iteration["+i+"]: "+ w)
    }

    println("Final separating plane: " + w)

    // display time elapsed for this job
    val t1 = System.currentTimeMillis()
    println("Elapsed time: "+ (t1-t0).toDouble*1e-3 +"seconds")
  }


  def isNumeric(line:String) : Boolean = {
    line.matches("""[-+]?[0-9]*\.?[0-9]+""")
  }

}
