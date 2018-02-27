package fanstop.rfm.model

import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree

/**
 * Created by yizhou on 2018/02/27.
 */
object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DecisionTree yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/labeledData/0227_uid/")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val c =trainingData.count()
    println("train count: " + c)
//sc.stop()
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 8
    val categoricalFeaturesInfo = Map[Int, Int]()
//    val impurity = "gini"
    val impurity = "entropy"
    val maxDepth = 3
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)
//    println(model.topNode.split.
    println(model.topNode.leftNode.toString)
    println(model.topNode.rightNode.toString)
//    println("tree model graph: " + model.)

    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

  }
}
