package dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * @Auther WeiJiQian
 * @Date 2020/11/9 11:04
 * @描述
 */
object Spark_UDTF {
  def main(args:Array[String]): Unit ={
    val conf :SparkConf = new SparkConf().setMaster("local[2]").setAppName("demo")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._
    val user = sparkSession.sparkContext.parallelize(Seq(
      User(1, "aa","bj")
      , User(2, "bb", "bj")
      , User(3, "aa", "sh")
      , User(4, "aa", "sh")
    )).toDS()

    user.groupByKey(u => u.province)
        .flatMapGroups((key,it) => {
          var array:ArrayBuffer[User] =new ArrayBuffer[User]()
          for (elem <- it) {
            array+=elem
          }
          array
        })
        .show()

    sparkSession.stop()


  }
}

case class User(id:Int,name:String,province:String)




