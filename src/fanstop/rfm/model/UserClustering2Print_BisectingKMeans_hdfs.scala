package fanstop.rfm.model

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by yizhou on 2018/04/09.
 */
object UserClustering2Print_BisectingKMeans_hdfs {
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
    val sparkConf = new SparkConf().setAppName("UserClustering yizhou")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")

    var i = 0
    args.foreach { x =>
      println("input " + i + ": " + x)
      i += 1
    }

    val labeledData_path = args(0)
    val model_path = args(1)
    // Load and parse the data
    val data = sc.textFile("/user_ext/ads_fanstop/yizhou/spark/fanstop/rfm/trainData/0409_uid").map(_.split("\\|")(0)).distinct()
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._

    val parsedData_train = data.map(s => Vectors.dense(s.split(" ").slice(1,4).map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 8
    val numIterations = 20

    val clusters = new BisectingKMeans().setK(numClusters).run(parsedData_train)
    clusters.save(sc, model_path)//保存模型
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData_train)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.predict(parsedData_train).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(numClusters).foreach(println)//不同group的个数

//sc.stop

    val df = data.map(x => TRAIN_DATA(x.split(" ")(0).toLong, x.split(" ")(1).toDouble, x.split(" ")(2).toDouble, x.split(" ")(3).toDouble)).toDF()
    val count = df.count()
    df.describe().show
    val sorted_r = df.select("r").sort("r").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }
    val sorted_log_f = df.select("log_f").sort("log_f").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }
    val sorted_log_m = df.select("log_m").sort("log_m").rdd.zipWithIndex().map { case (v, idx) => (idx, v) }

    val median_r = sorted_r.lookup(count / 2).head(0).formatted("%.2f").toDouble
    val four_quarter_log_f = sorted_log_f.lookup(count / 4 * 3).head(0).formatted("%.2f").toDouble
    val median_log_f = sorted_log_f.lookup(count / 4 * 3).head(0).formatted("%.2f").toDouble
    val median_log_m = sorted_log_m.lookup(count / 2).head(0).formatted("%.2f").toDouble


    val r_mean = df.describe("r").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_f_mean = df.describe("log_f").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_m_mean = df.describe("log_m").where("summary='mean'").head(1)(0)(1).toString.toDouble

    println("r_mean: "+r_mean)
    println("log_f_mean: "+log_f_mean)
    println("log_m_mean: "+log_m_mean)

    println("median_r: "+median_r)
    println("four_quarter_log_f: "+four_quarter_log_f)
    println("median_log_m: "+median_log_m)
    println("median_log_f: "+median_log_f)

    var cluster_id = -1
    val m:Map[Int, (String, String)] = clusters.clusterCenters.map{x=>
      val r = x(0).formatted("%.2f").toDouble
      val f = x(1).formatted("%.2f").toDouble
      val m = x(2).formatted("%.2f").toDouble
      var r_tag = 0
      var f_tag = 0
      var m_tag = 0

      //根据中位数,和4分位数
      if(r >= r_mean){
        r_tag=1
      }
      if(f >= four_quarter_log_f){
        f_tag=1
      }
      if(m >= median_log_m){
        m_tag=1
      }
//      println(r + " "+ f + " " + m + " " + "\t\t\t" + r_tag + " "+ f_tag + " " + m_tag)

      var rfm_tag=""
      var life_circle_tag=""
      if(r_tag == 0 && f_tag == 0 && m_tag == 0){
        rfm_tag="发展低价值"
        life_circle_tag="发展"
      }else if(r_tag == 0 && f_tag == 0 && m_tag == 1){
        rfm_tag="发展高价值"
        life_circle_tag="发展"
      }else if(r_tag == 0 && f_tag == 1 && m_tag == 0){
        rfm_tag="稳定低价值"
        life_circle_tag="稳定"
      }else if(r_tag == 0 && f_tag == 1 && m_tag == 1){
        rfm_tag="稳定高价值"
        life_circle_tag="稳定"
      }else if(r_tag == 1 && f_tag == 1 && m_tag == 1){
        rfm_tag="流失高频大客户"
        life_circle_tag="流失"
      }else if(r_tag == 1 && f_tag == 1 && m_tag == 0){
          rfm_tag="流失高频小客户"
          life_circle_tag="流失"
      }else if(r_tag == 1 && f_tag == 0 && m_tag == 0){
        rfm_tag="流失低频小客户"
        life_circle_tag="流失"
      }else if(r_tag == 1 && f_tag == 0 && m_tag == 1){
        rfm_tag="流失低频大客户"
        life_circle_tag="流失"
      }else{
        rfm_tag="未知"
        life_circle_tag="未知"
      }
      cluster_id+=1
      println(cluster_id+" "+r + " "+ f + " " + m + " " + "\t\t\t" + r_tag + " "+ f_tag + " " + m_tag+" "+rfm_tag+" "+life_circle_tag)
      (cluster_id,(rfm_tag,life_circle_tag))
    }.toMap

    data.map { x =>
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))
      val rfmArray = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble)).toArray
      val score = rfmArray(0)+2*rfmArray(1)+rfmArray(2)
      val label = clusters.predict(x_value)
      val s = m.get(label).getOrElse(0).toString

      val tag = s.substring(1,s.length-1).split(",")
      x.split(" ")(0) +" "+tag(0)+" "+tag(1)+" "+score+" "+x
    }.saveAsTextFile(labeledData_path)

  }
}