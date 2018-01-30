package UserSimilarityByTags.MyTest

/**
 * Created by yizhou on 2017/12/25.
 * 这个任务，由于sql占用的时间太长，并且内存溢出，不用了
 */

import org.apache.spark.sql.SparkSession


object KolFansTagsScore {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.rpc.message.maxSize", "512")
      .config("spark.rpc.netty.dispatcher.numThreads", "2")
      .enableHiveSupport()
      .getOrCreate()
    val sqlDF = spark.sql("select t1.uid,t3.tag from\n" +
      "(select uid from kol_uids where dt=20171206) t1\n" +
      "join\n" +
      "(select fans_uid,atten_uid from ods_user_fanslist where dt=20171224) t2\n" +
      "on t1.uid=t2.atten_uid\n" +
      "join\n" +
      "(select uid,tag FROM mds_ad_algo_user_tag where dt='working') t3\n" +
      "on t2.fans_uid=t3.uid")

    sqlDF.rdd.flatMap { x =>
      val uid = x(0).toString
      val tag_score: Array[String] = x(1).toString.split(",")
      tag_score.map { a =>
        val tag = a.split(":")(0)
        val score = a.split(":")(1)
        ((uid, tag), score.toInt)
      }
    }.reduceByKey(_+_).map(x=>(x._1._1+","+x._1._2+","+x._2)).saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/1226")

  }
}