package com.atguigu.bigdata.spark.core.rdd.operator

import org.apache.spark.{SparkConf, SparkContext}

object Spark_Transform_Part {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        val rdd = sc.makeRDD(List(1,2,3,4),2)
        // 【1，2】，【3，4】
        rdd.saveAsTextFile("output")
        val mapRDD = rdd.map(_*2)
        // 【2，4】，【6，8】
        mapRDD.saveAsTextFile("output1")

        sc.stop()

    }
}
