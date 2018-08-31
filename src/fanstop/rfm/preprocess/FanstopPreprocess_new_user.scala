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
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
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


    val spark = SparkSession
      .builder()
      .appName("Fanstop RFM Preprocess spark hive yizhou")
      .config("spark.some.config.option", "some-value")
//      .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
//      .config("spark.sql.warehouse.dir", "/dw/sds/")
//      .config("spark.sql.warehouse.dir", "/dw/ods/")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.rpc.message.maxSize", "512")
      .config("spark.rpc.netty.dispatcher.numThreads", "2")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    //    val sqlDF = spark.sql("se
    val sqlDF = spark.sql("select distinct(uid) from data_xunjia where dt >= "+start_day+" and uid not in (select distinct(uid) from sds_ad_headline_report_order_day where dt>="+start_day+")")
//    val sqlDF = spark.sql("select distinct(cust_uid) from ods_ad_headline_order_reads_info where dt>="+start_day)
    val new_user = sqlDF.rdd.map(x=>x(0).toString).distinct()
    new_user.map(x=>x+" 初入期 初入期 0.1").repartition(1).saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/new_user/"+getNowDate())


  }
}
