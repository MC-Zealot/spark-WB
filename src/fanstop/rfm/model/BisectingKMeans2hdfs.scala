package fanstop.rfm.model

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 每月训练聚类模型
  * Created by yizhou on 2018/04/11.
  */
object BisectingKMeans2hdfs {
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
  case class TRAIN_DATA(uid:Long, r:Double, log_f:Double, log_m:Double)//
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BisectingKMeans2hdfs yizhou")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")

    var i = 0
    args.foreach { x =>
      println("input " + i + ": " + x)
      i += 1
    }

    val model_path = args(1)
    // Load and parse the data
    val data = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/trainData/0409_uid").map(_.split("\\|")(0)).distinct()

    val parsedData_train = data.map(s => Vectors.dense(s.split(" ").slice(1,4).map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 8
    val numIterations = 30

    val clusters = new BisectingKMeans().setK(numClusters).run(parsedData_train)
    clusters.save(sc, model_path)//保存模型,备份
    clusters.save(sc, "/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/model/BisectingKMeans/working")//保存模型,使用
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData_train)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.predict(parsedData_train).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(numClusters).foreach(println)//不同group的个数

  }
}