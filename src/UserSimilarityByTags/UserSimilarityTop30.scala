package UserSimilarityByTags

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yizhou on 2018/01/10.
 * 找到广告主最匹配的kol
 * 广告主3w
 * Kol 25w
 *
 * 使用余弦相似度
 *
 * 1、每个kol，先遍历每个广告主
 * 2、计算广告主的人数，kol的总人数
 * 3、计算每个标签的概率（其实算一下pearson相似度也可以）
 * 4、cos(x,y)= X * Y / |X| * |Y| , X为广告主标签概率， Y为kol标签概率
 */
object UserSimilarityTop30 {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("cf item-based user similarity")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "512m")
    sparkConf.set("spark.rpc.message.maxSize", "512")
    sparkConf.set("spark.rpc.netty.dispatcher.numThreads", "2")
    val sc = new SparkContext(sparkConf)

    var i = 0
    args.foreach { x =>
      println("input " + i + ": " + x)
      i += 1
    }
    if( args.length <1){
      println("size is not enough:" + args.length)
      return 0;
    }
    val input = args(0)
    val output = args(1)


//    val all_user_similarity = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/similarity/cosine/20170111")
    val all_user_similarity = sc.textFile(input)
//    val all_count = all_user_similarity.count
//    val distinct_ad_count = all_user_similarity.map(_.split(",")(0)).distinct().count()
//    val distinct_kol_count = all_user_similarity.map(_.split(",")(1)).distinct().count()
//
//    println("================================all_count: "+all_count)
//    println("================================distinct_ad_count: "+distinct_ad_count)
//    println("================================distinct_kol_count: "+distinct_kol_count)
//    ================================all_count: 6619755604
//    ================================distinct_ad_count: 27346
//    ================================distinct_kol_count: 242074
    all_user_similarity.map(x=>(x.split(",")(0), x)).groupByKey().flatMap{x=>
      val uid = x._1
      val tags = x._2
      tags.map(x=>x.substring(1,x.length-1).replace(",","\t")).toSeq.sortWith(_.split("\t")(2).toDouble > _.split("\t")(2).toDouble).take(50)
    }.saveAsTextFile(output)

  }
}