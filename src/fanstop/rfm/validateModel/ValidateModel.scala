package fanstop.rfm.validateModel

/**
 * Created by yizhou on 2017/12/25
 * 获取用户（kol）粉丝的标签，
 * 1、计算用户各标签下，粉丝数、评分数
 * 2、取评分前50的标签
 *
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ValidateModel {
  /**
   * 时间戳转时间
    *
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
    val sparkConf = new SparkConf().setAppName("ValidateModel yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    val kmeansModel = KMeansModel.load(sc, "KMeansModel")
    println(kmeansModel)
    sc.stop()
    val data = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0227_uid").map(_.split("\\|")(0)).distinct()
    val oriMap = sc.textFile("file:///Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/data/201707_201802_all").filter(_.split("\t").length == 4).map { x =>
      val fields = x.split("\t")
      val uid = fields(1)

      (uid, x)
    }.groupByKey().collect().toMap

    data.take(10).map(x=>{
      val uid = x.split(" ")(0)
      val fields = x.split(" ")
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))
      val label = kmeansModel.predict(x_value)
      label+" "+x+"|"+oriMap.get(uid)
    }).foreach(println)

  }
}
