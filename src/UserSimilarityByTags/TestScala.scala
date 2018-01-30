package UserSimilarityByTags

/**
 * Created by yizhou on 2017/12/24.
 */

import java.util

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession


object TestScala {
  def main(args: Array[String]) {
//    val conf = new SparkConf();
//    conf.setMaster("local[2]")
//    conf.setAppName("user similarity")
//    val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
      .config("spark.local.dir", "${mapreduce.cluster.local.dir}")
//      .config("spark.local.dir", "${hadoop.tmp.dir}")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sqlDF = spark.sql("SELECT * FROM mds_ad_algo_user_tag where dt='working'")
    val local_set = sqlDF.flatMap(x=>x(1).toString.split(",")).map(x=>(x.split(":")(0))).distinct().rdd.collect().toSeq.toList

    //编码：tag-> map[tag, id]
//    val tag_list = sc.textFile("file:///Users/Zealot/yyt-git/SPARK_WB/src/tags/part-00000").collect.toList
    val tag_list = local_set
    println(tag_list.size)
    val id_tag_mapping = new util.HashMap[String, Int];//tag,id

    var a = 0;
    while (a < tag_list.size) {
      id_tag_mapping.put(tag_list(a), a)
      a += 1;
    }
    //tag的名称赋值成id
    sqlDF.flatMap(x=>{
      x(1).toString.split(",").map(tag_score=>{
        x(0)+","+id_tag_mapping.get(tag_score.split(":")(0))+","+tag_score.split(":")(1)
      })
    }).rdd.saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/user_tag_rating/1225")


    printf("hello ping an ye!\n")
  }
}
