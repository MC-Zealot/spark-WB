package fanstop.rfm.model

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 每月训练聚类模型
 * Created by yizhou on 2018/04/11.
 */
object BisectingKMeans2hdfs_bak {
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
  case class TRAIN_DATA( r:Double, log_f:Double, log_m:Double)//
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BisectingKMeans2hdfs yizhou")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")

    val spark = SparkSession
      .builder
      .appName("BisectingKMeans2hdfs yizhou")
      .getOrCreate()
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._

    var i = 0
    args.foreach { x =>
      println("input " + i + ": " + x)
      i += 1
    }

    val train_data_path = args(0)
    val model_path = args(1)
    // Load and parse the data
//    val data = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/trainData/0409_uid").map(_.split("\\|")(0)).distinct()
    val parsedData_train = sc.textFile(train_data_path).map(_.split("\\|")(0)).map{s =>
      val fileds = s.split(" ").map(_.toDouble).toList
      TRAIN_DATA(fileds(0), fileds(1), fileds(2))
    }.toDS()

    // Cluster the data into two classes using KMeans
    val numClusters = 8
    val numIterations = 30

    val clusters = new BisectingKMeans().setK(numClusters).setMaxIter(numIterations).fit(parsedData_train)

    clusters.save(model_path)//保存模型
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData_train)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.clusterCenters.foreach(println)
    clusters.transform(parsedData_train)
//    BisectingKMeans.load(model_path)
//    clusters.predict(dataset).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(numClusters).foreach(println)//不同group的个数


  }
}