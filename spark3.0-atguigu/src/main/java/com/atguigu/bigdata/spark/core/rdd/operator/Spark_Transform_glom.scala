package com.atguigu.bigdata.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Transform_glom {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - glom
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // List => Int
        // Int => Array
        val glomRDD: RDD[Array[Int]] = rdd.glom()

        glomRDD.collect().foreach(data=> println(data.mkString(",")))





        sc.stop()

    }
}
