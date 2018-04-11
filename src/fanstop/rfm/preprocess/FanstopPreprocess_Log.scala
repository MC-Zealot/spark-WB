package fanstop.rfm.preprocess

/**
 * Created by yizhou on 2018/02/23
 * rfm数据预处理
 * r：最近一次购买的时间距离当前时间的时间差，所以整体数据呈现线性分布
 * f：购买的次数。由于大部分用户只购买少量次数（5次以内，一个月），高于5次的用户数量很少，max为934（1个月），所以数据呈现非线性，需要进行非线性转换
 *max min标准化是线性转换，针对非线性数据无法把数据区分开
 * log函数转换标准化
 */

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.ml.feature.{MinMaxScaler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.linalg.Vectors

object FanstopPreprocess_Log {
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
  case class RFM(uid:String, r:Double,f:Double,m:Double, log_f:Double, log_m:Double)//

  /**
   * 计算时间差
   * @param start_time
   * @param end_Time
   * @return
   */
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

    val sparkConf = new SparkConf().setAppName("Fanstop RFM Preprocess yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")
    val sqlCon=new SQLContext(sc)
    val LOG_M = Math.log(10)
    val LOG_f = Math.log(2)
    import sqlCon.implicits._
    //粉条价格
    val c = sc.textFile("file:///Users/Zealot/Documents/data/201707_201802_all").distinct.
        filter(_.split("\t").length == 4).map { x =>
        val fields = x.split("\t")
        val uid = fields(1)
        val fentiao_price = fields(2).toDouble/1000 * 21
        val timestamps = fields(3)

        (uid, (fentiao_price.toDouble, 1, timestamps))
    }.reduceByKey { (x, y) =>
      var r3 = ""
      if (x._3 < y._3) {
        r3 = y._3
      } else {
        r3 = x._3
      }
      (x._1 + y._1, x._2 + y._2, r3)
    }.map{x=>
      val uid = x._1
      val expo = x._2._1
      val count = x._2._2
      val timestamps = x._2._3
      val recency = getCoreTime(parseDate(timestamps), getNowDate())

      (uid,recency,count,expo)
    }.filter(x=>(x._4 >= 1)).map{x=>
      val log_f = Math.log(x._3)/LOG_f//log标准化
      val log_m = Math.log(x._4)/LOG_M

      RFM(x._1,x._2,x._3,x._4, log_f, log_m)
    }
    val cdf = c.toDF()

    val r_max = cdf.describe("r").where("summary='max'").head(1)(0)(1).toString.toDouble
    val r_min = cdf.describe("r").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_r = r_max - r_min

    val log_f_max = cdf.describe("log_f").where("summary='max'").head(1)(0)(1).toString.toDouble
    val log_f_min = cdf.describe("log_f").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_f = log_f_max - log_f_min

    val log_m_max = cdf.describe("log_m").where("summary='max'").head(1)(0)(1).toString.toDouble
    val log_m_min = cdf.describe("log_m").where("summary='min'").head(1)(0)(1).toString.toDouble
    val diff_m = log_m_max - log_m_min

    val upper = 5
    val lower = 0
    val diff_bound = upper - lower

    cdf.describe().show
    sc.stop
    val rr_fenmu = diff_r / diff_bound //优化一下速度，不用每个map里边都再计算一次乘法
    val log_ff_fenmu = diff_f / diff_bound
    val log_mm_fenmu = diff_m / diff_bound
    c.map { x =>
      val uid = x.uid
      val rr = (x.r - r_min) / rr_fenmu//max min 标准化
//      val log_ff = (x.log_f - log_f_min) / log_ff_fenmu
//      val log_mm = (x.log_m - log_m_min) / log_mm_fenmu
        uid + " "+ rr + " " + x.log_f + " " + x.log_m+"|"+x
    }.
//      take(100).foreach(println)
      saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0309_uid")


  }
}
