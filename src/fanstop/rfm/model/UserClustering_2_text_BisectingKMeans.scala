package fanstop.rfm.model

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans, BisectingKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yizhou on 2018/02/09.
 */
object UserClustering_2_text_BisectingKMeans {
  case class TRAIN_DATA(uid:Long, r:Double, log_f:Double, log_m:Double)//
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserClustering yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")
    // Load and parse the data
    val data = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0226_uid").map(_.split("\\|")(0)).distinct()
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._

    val parsedData_train = data.map(s => Vectors.dense(s.split(" ").slice(1,4).map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 8
    val numIterations = 20

    //选择err下降比较多的k：6
//    var a = ArrayBuffer[String]()
//        val ks:Array[Int] = Array(3,4,5,6,7,8,9,10)
//        ks.foreach(cluster => {
//          val model = new BisectingKMeans().setK(cluster).run(parsedData_train)
////          val model:KMeansModel = KMeans.train(parsedData_train, cluster,20)
//          val ssd = model.computeCost(parsedData_train)
//          a+=ssd.toString
//        })
//        println(a)
//        sc.stop()

//    val clusters = KMeans.train(parsedData_train, numClusters, numIterations)
    val clusters = new BisectingKMeans().setK(numClusters).run(parsedData_train)


    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData_train)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    clusters.predict(parsedData_train).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(numClusters).foreach(println)//不同group的个数
    data.take(50).foreach { x =>
      val uid = x.split(" ")(0)
      val fields = x.split(" ")
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))

      val label = clusters.predict(x_value)
      println(label+" "+x)
//      println("{x:"+fields(1)+",y:"+fields(2)+",z:"+fields(3)+",color:\""+color+"\"},")
    }
    val df = data.map(x=>TRAIN_DATA(x.split(" ")(0).toLong,x.split(" ")(1).toDouble,x.split(" ")(2).toDouble,x.split(" ")(3).toDouble)).toDF()
    df.describe().show
    data.map{ x =>
      val uid = x.split(" ")(0)
      val fields = x.split(" ")
      val x_value = Vectors.dense(x.split(" ").slice(1, 4).map(_.toDouble))

      val label = clusters.predict(x_value)
//      println(label+" "+x)
      label+" 0:"+fields(1)+" 1:"+fields(2)+" 2:"+fields(3)
      //      println("{x:"+fields(1)+",y:"+fields(2)+",z:"+fields(3)+",color:\""+color+"\"},")
    }
      .repartition(1).saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/labeledData/0226_uid")

    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
