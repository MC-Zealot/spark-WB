package UserSimilarityByTags

/**
 * Created by yizhou on 2017/12/25
 * 获取用户（kol）粉丝的标签，
 * 1、计算用户各标签下，粉丝数、评分数
 * 2、取评分前50的标签
 *
 */

import java.util

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession


object KolFansTagsScore_new {
  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
//      .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryoserializer.buffer.max", "512m")
//      .config("spark.rpc.message.maxSize", "512")
//      .config("spark.rpc.netty.dispatcher.numThreads", "2")
//      .enableHiveSupport()
//      .getOrCreate()

//    import spark.implicits._
//    val sqlDF = spark.sql("select uid,fans_tags from uid_fans_tags where dt=20180131")
////得到uid,tag，score，fans_count，
//    sqlDF.rdd.flatMap { x =>
//      x(1).toString.split(",").map { a =>
//        val fields = a.split(":")
////        ((x(0),  fields(0)), 1)
//        ((x(0),  fields(0)),( fields(1).substring(0,fields(1).length -3).toInt, 1))
//      }
//    }.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1._1+"\t"+x._1._2+"\t"+x._2._1+"\t"+x._2._2)).saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/0131")
////    }.reduceByKey(_+_).repartition(300).map(x=>(x._1._1+"\t"+x._1._2+"\t"+x._2)).saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/0103")


//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("cf item-based yizhou")
    val sc = new SparkContext(sparkConf)
    sc.textFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/0131").filter(_.split("\t").length==4).filter(_.split("\t")(3).toInt>=50).repartition(100).
      map(x=>(x.split("\t")(0),x)).groupByKey().map{x=>
      val uid = x._1
      val tags = x._2
      tags.toSeq.sortWith(_.split("\t")(3).toInt > _.split("\t")(3).toInt).take(50)
    }.
      saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/filtered/0131")
  }
}
