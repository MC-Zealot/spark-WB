package fanstop.rfm.preprocess

/**
 * Created by yizhou on 2017/12/25
 * 获取用户（kol）粉丝的标签，
 * 1、计算用户各标签下，粉丝数、评分数
 * 2、取评分前50的标签
 *
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object FanstopPreprocess {
  /**
   * 时间戳转时间
   * @param time
   * @return
   */
  def parseDate(time:String):String={
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }
  def main(args: Array[String]) {

//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("FanstopPreprocess yizhou").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val c = sc.wholeTextFiles("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/data/").
      filter(_._1.endsWith("fans_new")).
      flatMap(_._2.split("\n"))
      .map{x=>
      val fields = x.split("\t")
      val uid = fields(1)
      val expo = fields(2)
      val timestamps = fields(3)

      (uid, (expo.toInt, 1, timestamps))
    }
      .reduceByKey { (x, y) =>
      var r3 = ""
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

            x._1 + "\t" + expo + "\t" + count + "\t" + parseDate(timestamps)
//      x._1 + "\t" + expo + "\t" + count + "\t" + timestamps
    }

    c.collect.foreach(println)
println(c.count)
  }
}
