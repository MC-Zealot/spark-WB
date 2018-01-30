package UserSimilarityByTags.MyTest

/**
 * Created by yizhou on 2017/12/24.
 */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object HelloWorld {
   def main(args: Array[String]) {
     val conf = new SparkConf();
     val spark = SparkSession
       .builder()
       .appName("Spark SQL basic example")
       .config("spark.some.config.option", "some-value")
       .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
       .enableHiveSupport()
       .getOrCreate()

     import spark.implicits._
     val sqlDF = spark.sql("SELECT * FROM mds_ad_algo_user_tag where dt='working'")
     sqlDF.flatMap(x=>x(1).toString.split(",")).map(x=>(x.split(":")(0))).distinct().rdd.repartition(1).saveAsTextFile("file:////data0/ads_fanstop/yizhou/spread_influence/script/spark/tags")
 //    sqlDF.show()
     printf("hello ping an ye!\n")
   }
 }
