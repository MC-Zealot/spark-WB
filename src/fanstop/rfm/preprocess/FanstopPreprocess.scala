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

import org.apache.spark.ml.feature.{StandardScaler, QuantileDiscretizer}
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.sql.SQLContext
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
//    val decf: DecimalFormat = new DecimalFormat("#.00")
//    decf.format(hour) //格式化
    day
  }

  def main(args: Array[String]) {

//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("FanstopPreprocess yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    val c = sc.wholeTextFiles("file:///Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/data/").
      filter(_._1.endsWith("all_fans")).
      flatMap(_._2.split("\n")).map{x=>
      val fields = x.split("\t")
      val uid = fields(1)
      val expo = fields(2)
      val timestamps = fields(3)

      (uid, (expo.toInt, 1, timestamps))
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
             val a = recency + " " + count + " " + expo
      a.split(" ")
    }.map{x=>
      RFM(x(0).toDouble,x(1).toDouble,x(2).toDouble)
    }
    val cdf = c.toDF()
//    cdf.show(100)
//    val mean =cdf.describe("r","f","m").where("summary='mean'")
//    mean.show()
//    val r_mean = mean.select("r").collect
//    val f_mean = mean.select("f").collect
//    val m_mean = mean.select("m").collect
//    println(r_mean(0))
//    println(f_mean(0))
//    println(m_mean(0))

    val rdf = new QuantileDiscretizer()
      .setInputCol("r")
      .setOutputCol("rCategory")
      .setNumBuckets(5)//设置分箱数
      .setRelativeError(0.1)//设置precision-控制相对误差
      .fit(cdf)
      .transform(cdf);

    val mdf = new QuantileDiscretizer()
      .setInputCol("m")
      .setOutputCol("mCategory")
      .setNumBuckets(5)//设置分箱数
      .setRelativeError(0.1)//设置precision-控制相对误差
      .fit(rdf)
      .transform(rdf);

    val fdf = new QuantileDiscretizer()
      .setInputCol("f")
      .setOutputCol("fCategory")
      .setNumBuckets(3)//设置分箱数
      .setRelativeError(0.1)//设置precision-控制相对误差
      .fit(mdf)
      .transform(mdf)
//    val cc = fdf.filter("f>1")
    fdf.show(100)
    println(fdf.count)
//    c.collect.foreach(println)
//println(c.count)
    fdf.select("rCategory","fCategory","mCategory").rdd.map{x=>
    val l = x.toString().length
      x.toString().substring(1,l-1).replace(","," ")
  }.saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0211")


  }
}
