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
object UserSimilarity {

  def similarity(t1: Map[String, Int], t2: Map[String, Int]): Double = {
    //word, t1 freq, t2 freq
    val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

    val sum1 = t1.foldLeft(0d) {case (sum, (word, freq)) =>
      m += word ->(freq, 0)
      sum + freq
    }

    val sum2 = t2.foldLeft(0d) {case (sum, (word, freq)) =>
      m.get(word) match {
        case Some((freq1, _)) => m += word ->(freq1, freq)//相同的词，不同的频率保存到m里边。
        case None => m += word ->(0, freq)
      }
      sum + freq
    }

    val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
      val fs = e._2
      val f1 = fs._1 / sum1
      val f2 = fs._2 / sum2
      (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
    }

    val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
    cos
  }

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


    val ad_uids = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/data/fst_uids")
    val kol_uids = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/data/kol_uids").filter(_.split("\t").length==2).map(_.split("\t")(0))

    val filtered_ratings = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/kol_ratings/filtered/0104_2").
      map(x=>{
      val x2 = x.substring(5,x.length-1)
      (x2.split("\t")(0),x2)
    })

    val kol_ratings = filtered_ratings.join(kol_uids.map(x=>(x,1))).map(x=>(x._1,x._2._1.split(",").map(x=>(x.split("\t")(1),x.split("\t")(3).toInt)))) //uid,   List(uid,tag,rating,fans_count)
    val ad_ratings = filtered_ratings.join(ad_uids.map(x=>(x,1))).map(x=>(x._1,x._2._1.split(",").map(x=>(x.split("\t")(1),x.split("\t")(3).toInt))))


    ad_ratings.cartesian(kol_ratings).map(x=>{
      val ad_uid = x._1._1
      val kol_uid = x._2._1

      val ad_tag_ratings = x._1._2.toMap //tag,ratings
      val kol_tag_ratings = x._2._2.toMap

      (ad_uid,kol_uid,similarity(ad_tag_ratings,kol_tag_ratings))
    }).saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/similarity/cosine/20170129")

  }
}