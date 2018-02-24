package fanstop.rfm.model


import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by yizhou on 2018/02/09.
 */
object UserClustering {
  case class TRAIN_DATA(uid:Long, r:Double, log_f:Double, log_m:Double)//
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserClustering yizhou").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // Load and parse the data
    val data = sc.textFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/trainData/0223_uid").distinct()
    val sqlCon=new SQLContext(sc)
    import sqlCon.implicits._

    val parsedData_train = data.map(s => Vectors.dense(s.split(" ").slice(1,4).map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 7
    val numIterations = 20

    //选择err下降比较多的k：6
//    var a = ArrayBuffer[String]()
    //    val ks:Array[Int] = Array(3,4,5,6,7,8)
    //    ks.foreach(cluster => {
    //      val model:KMeansModel = KMeans.train(parsedData_train, cluster,20)
    //      val ssd = model.computeCost(parsedData_train)
    //      a+=ssd.toString
    //    })
    //    println(a)
    //    sc.stop()

    val clusters = KMeans.train(parsedData_train, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData_train)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    clusters.predict(parsedData_train).map(x=>(x,1)).reduceByKey(_+_).sortByKey(false).take(numClusters).foreach(println)//不同group的个数
//sc.stop
    data.take(500).foreach { x =>
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
    val df = data.map(x=>TRAIN_DATA(x.split(" ")(0).toLong,x.split(" ")(1).toDouble,x.split(" ")(2).toDouble,x.split(" ")(3).toDouble)).toDF()
    df.describe().show
    val r_mean = df.describe("r").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_f_mean = df.describe("log_f").where("summary='mean'").head(1)(0)(1).toString.toDouble
    val log_m_mean = df.describe("log_m").where("summary='mean'").head(1)(0)(1).toString.toDouble
    clusters.clusterCenters.foreach{x=>
      val r = x(0)
      val f = x(1)
      val m = x(2)
      var r_tag = "-"
      var f_tag = "-"
      var m_tag = "-"
      if(r > r_mean){
        r_tag="+"
      }
      if(f > log_f_mean){
        f_tag="+"
      }
      if(m > log_m_mean){
        m_tag="+"
      }
      println(x+"\t\t\t"+ r_tag+" "+ f_tag+" "+ m_tag)
    }//不同group的中心点
//      repartition(1).saveAsTextFile("/Users/Zealot/yyt-git/SPARK_WB/src/fanstop/rfm/labeledData/0223_uid")

    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
