package fanstop.rfm.model

import java.text.SimpleDateFormat
import java.util.Date

import fanstop.rfm.model.UserClustering2Print_kmeans.TRAIN_DATA
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
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
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    var i = 0
    args.foreach { x =>
      println("input " + i + ": " + x)
      i += 1
    }

    val model_path = args(1)
    val train_path = args(0)
    // Load and parse the data
//    val data = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/trainData/0409_uid").map(_.split("\\|")(0)).distinct()
    val data = sc.textFile(train_path).map(_.split("\\|")(0)).distinct()

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

    data.take(100).foreach { x =>
      val uid = x.split(" ")(0)
      val fields = x.split(" ")
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))

      val label = clusters.predict(x_value)
      //      val r_mean =
      var color=""
      if(label==0){
        color="red"
      }else if(label==1){
        color="yellow"
      }else if(label==2){
        color="blue"
      }else if(label==3){
        color="green"
      }else if(label==4){
        color="black"
      }else if(label==5){
        color="orange"
      }else if(label==6){
        color="purple"
      }else if(label==7){
        color="brown"
      }else {
        color="grey"
      }
      //      println(label+" "+x)
      println("{x:"+fields(1)+",y:"+fields(2)+",z:"+fields(3)+",color:\""+color+"\"},")
    }
    data.take(50).foreach { x =>
      val uid = x.split(" ")(0)
      val fields = x.split(" ")
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))

      val label = clusters.predict(x_value)
      //      val r_mean =
      var color=""
      if(label==0){
        color="red"
      }else if(label==1){
        color="yellow"
      }else if(label==2){
        color="blue"
      }else if(label==3){
        color="green"
      }else if(label==4){
        color="black"
      }else if(label==5){
        color="orange"
      }else if(label==6){
        color="purple"
      }else{
        color="brown"
      }
      println(label+" "+x)
      //      println("{x:"+fields(1)+",y:"+fields(2)+",z:"+fields(3)+",color:\""+color+"\"},")
    }
    val df = data.map(x => TRAIN_DATA(x.split(" ")(0).toLong, x.split(" ")(1).toDouble, x.split(" ")(2).toDouble, x.split(" ")(3).toDouble)).toDF()
    val count = df.count()
    df.describe().show
    val sorted_r = df.select("r").sort("r").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }
    val sorted_log_f = df.select("log_f").sort("log_f").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }
    val sorted_log_m = df.select("log_m").sort("log_m").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }

    val median_r = sorted_r.lookup(count / 2).head(0).toString().toDouble
    val median_log_f = sorted_log_f.lookup(count / 2).head(0).toString().toDouble
    val median_log_m = sorted_log_m.lookup(count / 2).head(0).toString().toDouble


    val r_mean = df.describe("r").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_f_mean = df.describe("log_f").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_m_mean = df.describe("log_m").where("summary='mean'").head(1)(0)(1).toString.toDouble

    println("r_mean: "+r_mean)
    println("log_f_mean: "+log_f_mean)
    println("log_m_mean: "+log_m_mean)

    println("median_r: "+median_r)
    println("median_log_f: "+median_log_f)
    println("median_log_m: "+median_log_m)

    clusters.clusterCenters.foreach{x=>
      val r = x(0)
      val f = x(1)
      val m = x(2)
      var r_tag = "-"
      var f_tag = "-"
      var m_tag = "-"
      if(r > median_r){
        r_tag="+"
      }
      if(f > log_f_mean){
        f_tag="+"
      }
      if(m > median_log_m){
        m_tag="+"
      }
      println(x+"\t\t\t"+ r_tag+" "+ f_tag+" "+ m_tag)
    }
  }
}