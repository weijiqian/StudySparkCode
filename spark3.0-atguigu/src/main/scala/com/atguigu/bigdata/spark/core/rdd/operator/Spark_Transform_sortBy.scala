package com.atguigu.bigdata.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Transform_sortBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //  算子 - sortBy  全局排序
        val rdd = sc.makeRDD(List(6,2,4,5,3,1), 2)

        val newRDD: RDD[Int] = rdd.sortBy(num=>num)

        newRDD.saveAsTextFile("output")




        sc.stop()

    }
}
