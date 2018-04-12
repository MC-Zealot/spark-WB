package fanstop.rfm.analysis

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 成单率计算
  * Created by yizhou on 2018/03/08.
  */
object Chengdanlv {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("chengdanlv yizhou")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("error")

    //    成单数，购买数
    sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_chengdan/all").map(x=>(x.split("\t")(1),1)).reduceByKey(_+_).map(x=>(x._2,1)).reduceByKey(_+_).sortByKey(true).take(10).foreach(println)
    //    每个用户的成单数
    val user_buy_count = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_chengdan/all").map(x=>(x.split("\t")(1),1)).reduceByKey(_+_).collect().toSeq.toMap

    val all_chengdan_users = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data/all").map(_.split("\t")(1)).distinct()
    val all_xunjia_users = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_xunjia/all").map(_.split("\t")(0)).distinct()
    val new_users = all_xunjia_users.subtract(all_chengdan_users).collect().toSet
    val all_chengdan_users_count = all_chengdan_users.count()
    val all_xunjia_users_count = all_xunjia_users.count()
    val new_users_size = new_users.size
    println("output=========all_chengdan_users_count: "+ all_chengdan_users_count)
    println("output=========all_xunjia_users_count: "+ all_xunjia_users_count)
    println("output=========new_users_size: "+ new_users_size+", getSimpleName: "+new_users.getClass.getSimpleName)

    var dayy = 1
    while (dayy <= 7) {
      var count = 0
      val count_chengdan_0 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data/201803/2018030" + dayy + "_fans").filter(x => {
        new_users.contains(x.split("\t")(1))
      }).map(_.split("\t")(1)).distinct().count()

      val count_xunjia_0 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_xunjia/2018030" + dayy + ".data").filter(x => {
        new_users.contains(x.split("\t")(0))
      }).map(_.split("\t")(0)).distinct().count

      val chengdanlv_0 = count_chengdan_0.toDouble / count_xunjia_0
      println("output================2018030" + dayy + ", count: "+count+": " + count_chengdan_0 + "\t" + count_xunjia_0 + "\t" + chengdanlv_0.formatted("%.3f"))

      count = 1
      while (count <= 5) {

        //3月1号购买一次的用户数
        val count_chengdan_1 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_chengdan/2018030" + dayy + "_nonfans").filter(x => {
          user_buy_count.get(x.split("\t")(1)).getOrElse(0) == count
        }).map(_.split("\t")(1)).distinct().count

        val count_xunjia_1 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_xunjia/2018030" + dayy + ".data").filter(x => {
          user_buy_count.get(x.split("\t")(0)).getOrElse(0) == count
        }).map(_.split("\t")(0)).distinct().count
        val chengdanlv_1 = count_chengdan_1.toDouble / count_xunjia_1
        println("output================2018030" + dayy + ", count: "+count+": " + count_chengdan_1 + "\t" + count_xunjia_1 + "\t" + chengdanlv_1.formatted("%.3f"))

        count = count + 1
      }

      val count_chengdan_1 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_chengdan/2018030" + dayy + "_nonfans").filter(x => {
        user_buy_count.get(x.split("\t")(1)).getOrElse(0) >= count
      }).map(_.split("\t")(1)).distinct().count

      val count_xunjia_1 = sc.textFile("file:///data0/ads_fanstop/yizhou/fanstop/data_xunjia/2018030" + dayy + ".data").filter(x => {
        user_buy_count.get(x.split("\t")(0)).getOrElse(0) >= count
      }).map(_.split("\t")(0)).distinct().count
      val chengdanlv_1 = count_chengdan_1.toDouble / count_xunjia_1
      println("output================2018030" + dayy + ", count: "+count+": " + count_chengdan_1 + "\t" + count_xunjia_1 + "\t" + chengdanlv_1.formatted("%.3f"))
//
      dayy = dayy + 1
      println("dayy: "+dayy)
    }



  }
}
