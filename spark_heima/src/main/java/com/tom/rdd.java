package com.tom;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import scala.Function1;

/**
 * @Auther WeiJiQian
 * @Date 2020/8/17 22:20
 * @描述
 */
public class rdd {

    SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op");
    SparkContext sc = new SparkContext(conf);
    /**
     * CombineByKey 这个算子中接收三个参数:
     * 转换数据的函数(初始函数, 作用于第一条数据, 用于开启整个计算), 在分区上进行聚合, 把所有分区的聚合结果聚合为最终结果
     */
    @Test
    public void  combineByKey(){

        RDD<String> rdd = sc.textFile("", 3);
        rdd.map(new Function1<String, Object>() {
            @Override
            public Object apply(String v1) {
                return null;
            }

            @Override
            public <A> Function1<A, Object> compose(Function1<A, String> g) {
                return super.compose(g);
            }

            @Override
            public <A> Function1<String, A> andThen(Function1<Object, A> g) {
                return super.andThen(g);
            }
        })
//        // 2. 算子操作
//        //   2.1. createCombiner 转换数据
//        //   2.2. mergeValue 分区上的聚合
//        //   2.3. mergeCombiners 把所有分区上的结果再次聚合, 生成最终结果
//        val combineResult = rdd.combineByKey(
//                createCombiner = (curr: Double) => (curr, 1),
//        mergeValue = (curr: (Double, Int), nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
//        mergeCombiners = (curr: (Double, Int), agg: (Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
//    )
//        // ("zhangsan", (99 + 96 + 97, 3))
//        val resultRDD = combineResult.map( item => (item._1, item._2._1 / item._2._2) )
//
//        // 3. 获取结果, 打印结果
//        resultRDD.collect().foreach(println(_))
    }
}
