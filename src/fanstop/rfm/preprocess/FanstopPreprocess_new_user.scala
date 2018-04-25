package fanstop.rfm.preprocess

/**
 * Created by yizhou on 2018/04/25
获取新用户
 *
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object FanstopPreprocess_new_user {
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
    val start_day=args(0).toInt
//做一个过滤，粉丝数》50，标签保留人数的top50
    val sparkConf = new SparkConf().setAppName("FanstopPreprocess yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    val xunjia = sc.wholeTextFiles("file:///data0/ads_fanstop/yizhou/fanstop/data_xunjia/").
      filter(_._1.split(".")(0).toInt > start_day).
      flatMap(_._2.split("\n")).map{x=>
      val fields = x.split("\t")
      val uid = fields(1)
     uid.toInt
    }.distinct()
    val spark = SparkSession
      .builder()
      .appName("Fanstop RFM Preprocess spark hive yizhou")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.rpc.message.maxSize", "512")
      .config("spark.rpc.netty.dispatcher.numThreads", "2")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    //    val sqlDF = spark.sql("se
    val sqlDF = spark.sql("select cust_uid,consume,post_date from sds_ad_headline_report_order_day where dt >= "+ start_day)
    val goumai = sqlDF.rdd.map(x=>x(0).toString.toInt).distinct()
    val new_user = xunjia.subtract(goumai)
    new_user.map(x=>x+" 引入期   0").repartition(1).saveAsTextFile("file:///data0/ads_fanstop/yizhou/fanstop/new_user/"+getNowDate())


  }
}
