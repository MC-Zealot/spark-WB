package fanstop.rfm.preprocess

/**
 * Created by yizhou on 2017/12/25
 * 获取用户（kol）粉丝的标签，
 * 1、计算用户各标签下，粉丝数、评分数
 * 2、取评分前50的标签
 *
 */

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.ml.feature.{MinMaxScaler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.linalg.Vectors

object FanstopPreprocess_MaxMin {
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
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var hehe = dateFormat.format( now )
    hehe
  }
  case class RFM(r:Double,f:Double,m:Double)//

  //核心工作时间，迟到早退等的的处理
  def getCoreTime(start_time: String, end_Time: String) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val begin: Date = df.parse(start_time)
    val end: Date = df.parse(end_Time)
    val between: Long = (end.getTime() - begin.getTime()) / 1000 //转化成秒
    val hour: Float = between.toFloat / 3600
    val day: Int = hour.toInt / 24
    day
  }

  def main(args: Array[String]) {

//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("FanstopPreprocess yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    val c = sc.wholeTextFiles("file:///Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/data/").distinct().
      filter(_._1.endsWith("all_fans")).
      flatMap(_._2.split("\n")).map{x=>
      val fields = x.split("\t")
      val uid = fields(1)
      val expo = fields(2)
      val timestamps = fields(3)

      (uid, (expo.toDouble, 1, timestamps))
    }.reduceByKey { (x, y) =>
      var r3 = ""
      if (x._3 < y._3) {
        r3 = y._3
      } else {
        r3 = x._3
      }
      (x._1 + y._1, x._2 + y._2, r3)
    }.map{x=>
      val expo = x._2._1
      val count = x._2._2
      val timestamps = x._2._3
      val recency = getCoreTime(parseDate(timestamps), getNowDate())
//             val a = recency + " " + count + " " + expo
//      a.split(" ")
      Array(recency,count,expo)
    }.map{x=>
      RFM(x(0),x(1),x(2))
    }

    c.take(10).foreach(println)
    val cdf = c.toDF()

//    val r_max = cdf.describe("r").where("summary='max'").head(1)(0)(1).toString.toDouble
    val r_max = cdf.describe("r").where("summary='max'").head(1)(0)(1).toString.toDouble
    val r_min = cdf.describe("r").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_r = r_max - r_min

    val f_max = cdf.describe("f").where("summary='max'").head(1)(0)(1).toString.toDouble
    val f_min = cdf.describe("f").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_f = f_max - f_min

    val m_max = cdf.describe("m").where("summary='max'").head(1)(0)(1).toString.toDouble
    val m_min = cdf.describe("m").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_m = m_max - m_min

    val upper = 5
    val lowwer = 0
    val diff_bound = upper - lowwer

    cdf.describe().show

    c.map { x =>
      val rr = (x.r - r_min) / diff_r * diff_bound
      var ff = 0.0
      var mm = 0.0
      val fenduan_threshold_f = 10
      val fenduan_threshold_m = 1.761363432594561E9
      if(x.f > fenduan_threshold_f){
        ff = (x.f - fenduan_threshold_f) / (f_max - fenduan_threshold_f) + 4
      }else{
        ff = (x.f - f_min) / (fenduan_threshold_f - f_min) * diff_bound
      }

      if(x.m > fenduan_threshold_m){
        mm = (x.m - fenduan_threshold_m) / (m_max - fenduan_threshold_m) + 4
      }else{
        mm = (x.m - m_min) /  (fenduan_threshold_m - m_min) * diff_bound
      }


     x+"|"+ rr + " " + ff + " " + mm
//      rr + " " + ff + " " + mm
    }.
      take(100).foreach(println)
//      repartition(1).saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0223")


  }
}
