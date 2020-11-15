package dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;


/**
 * @Auther WeiJiQian
 * @Date 2020/11/9 11:34
 * @描述
 */
public class Spark_GroupBykey {
    public static void main(String[] args) {
        SparkConf conf  = new SparkConf().setMaster("local[*]").setAppName("demo");
        SparkSession sparkSession  = SparkSession.builder().config(conf).getOrCreate();


//        List<User> list = new ArrayList<User>();
//        list.add(new User(1, "aa","bj"));
//        list.add(new User(2, "bb","bj"));
//        list.add(new User(3, "aa","sh"));
//        list.add(new User(4, "aa","sh"));
//
//        JavaRDD<User> userRDD = sparkSession.sparkContext().parallelize(list).toJavaRDD();
//
//        userRDD.groupBy()

    }
}
