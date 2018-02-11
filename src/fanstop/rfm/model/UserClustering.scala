package fanstop.rfm.model

import java.util

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yizhou on 2018/02/09.
 */
object UserClustering {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserClustering yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // Load and parse the data
//    val data = sc.textFile("/Users/Zealot/dev/spark-2.0.0-bin-hadoop2.4/data/mllib/kmeans_data.txt")
    val data = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0211")
    val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 6
    val numIterations = 30

    //选择err下降比较多的k：6
//    var a = ArrayBuffer[String]()
//    val ks:Array[Int] = Array(3,4,5,6,7,8)
//    ks.foreach(cluster => {
//      val model:KMeansModel = KMeans.train(parsedData, cluster,20,1)
//      val ssd = model.computeCost(parsedData)
//      a+=ssd.toString
//    })
//    println(a)


    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.clusterCenters.foreach(println)
    clusters.predict(parsedData).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(10).foreach(println)
//    {x:1,y:6,z:5,color:"red"}
    parsedData.map{x=>
      val label = clusters.predict(x)
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
        color="purple"
      }else if(label==5){
        color="orange"
      }
      "{x:"+x(0)+",y:"+x(1)+",z:"+x(2)+",color:\""+color+"\"},"
    }.repartition(1).distinct.saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/labeledData/0211_3")

    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
