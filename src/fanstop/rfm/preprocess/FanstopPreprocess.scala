package fanstop.rfm.preprocess

/**
 * Created by yizhou on 2017/12/25
 * 获取用户（kol）粉丝的标签，
 * 1、计算用户各标签下，粉丝数、评分数
 * 2、取评分前50的标签
 *
 */

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.{SparkContext, SparkConf}


object FanstopPreprocess {
  def main(args: Array[String]) {

//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("cf item-based yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val c = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/data/20180131_fans").map{x=>
      val fields = x.split("\t")
      val uid = fields(1)
      val expo = fields(2)
      val timestamps = fields(3)
      (uid,(expo.toInt,1,timestamps.toInt))
    }.reduceByKey { (x, y) =>
      var r3 = 0
      if (x._3 < y._3) {
        r3 = y._3
      } else {
        r3 = x._3
      }
      (x._1 + y._1, x._2 + y._2, r3)
    }
    .map{x=>
      val expo = x._2._1
      val count = x._2._2
      val timestamps = x._2._3
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date: String = df.format(timestamps.toLong)

      x._1 + "\t" + expo + "\t" + count + "\t" + date
    }

    c.take(10).foreach(println)

  }
}
