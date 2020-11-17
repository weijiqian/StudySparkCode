package com.atguigu.bigdata.spark.core.rdd.operator

import org.apache.spark.{SparkConf, SparkContext}

object Spark_Transform_mapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // 【1，2】，【3，4】
        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        )
        mpiRDD.collect().foreach(println)


        sc.stop()

    }
}
