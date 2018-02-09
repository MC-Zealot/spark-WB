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
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
