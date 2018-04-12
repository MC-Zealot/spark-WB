package UserSimilarityByTags.MyTest

/**
 * Created by yizhou on 2017/12/24.
 */

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.ml.clustering.{BisectingKMeansModel, BisectingKMeans}

object HelloWorld {
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()
    tim
  }
  def main(args: Array[String]) {
    //     val conf = new SparkConf();
    //     val spark = SparkSession
    //       .builder()
    //       .appName("Spark SQL basic example")
    //       .config("spark.some.config.option", "some-value")
    //       .config("spark.sql.warehouse.dir", "/dw_ext/ad/mds/")
    //       .enableHiveSupport()
    //       .getOrCreate()
    //
    //     import spark.implicits._
    //     val sqlDF = spark.sql("SELECT * FROM mds_ad_algo_user_tag where dt='working'")
    //     sqlDF.flatMap(x=>x(1).toString.split(",")).map(x=>(x.split(":")(0))).distinct().rdd.repartition(1).saveAsTextFile("file:////data0/ads_fanstop/yizhou/spread_influence/script/spark/tags")
    // //    sqlDF.show()
    printf("hello ping an ye!\n")



//

    val sparkConf = new SparkConf().setAppName("BisectingKMeans2hdfs yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")

    val spark = SparkSession
      .builder
      .appName("BisectingKMeans2hdfs yizhou")
      .getOrCreate()
    val sqlCon=new SQLContext(sc)
    // Loads data.
    val dataset = spark.read.format("libsvm").load("/Users/Zealot/dev/spark-2.0.0-bin-hadoop2.4/data/mllib/sample_kmeans_data.txt")

    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(dataset)

    // Evaluate clustering.
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)
    model.transform(dataset).rdd.take(10).foreach{x=>
    println(x.get(2))


      val clusters = BisectingKMeansModel.load("") //加载模型
      clusters.clusterCenters.foreach(println)
      BisectingKMeansModel
    }

  }
}
