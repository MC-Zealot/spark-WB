package fanstop.rfm.model

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yizhou on 2018/02/09.
 */
object UserClustering {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserClustering yizhou").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    // Load and parse the data
//    val data = sc.textFile("/Users/Zealot/dev/spark-2.0.0-bin-hadoop2.4/data/mllib/kmeans_data.txt")
    val data = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 8
    val numIterations = 200
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.clusterCenters.foreach(println)


    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
