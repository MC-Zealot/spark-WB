package com.item_based

/**
 * Created by Administrator on 2016/1/8.
 */

import java.io.{File, PrintWriter}

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object item_based_similarity {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("cf item-based yizhou")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "512m");
    sparkConf.set("spark.rpc.message.maxSize", "512");
    sparkConf.set("spark.rpc.netty.dispatcher.numThreads", "2");
    val sc = new SparkContext(sparkConf)

    sc.textFile("file:///Users/Zealot/Desktop/papijiang").filter(_.split(",").size>1).filter(!_.contains("\"")).map(x=>{(x.split(",")(0),x.split(",")(1).toInt)}).reduceByKey(_+_).sortBy(_._2, false).collect.foreach(println)

    val user_rdd = UserData(sc, "/user_ext/ads_fanstop/yizhou/spark/user_similarity/user_tag_rating/1225", ",")
    val coor = EuclideanDistanceSimilarity_User(user_rdd)
    coor.saveAsTextFile("/user_ext/ads_fanstop/yizhou/spark/user_similarity/simi_result/1225")
//    val coor = Cooccurrence(user_rdd)
    // var coor1 = Cooccurrence_Euclidean(user_rdd)

    //  val result1 = Recommend(coor1,user_rdd,20)
    val result = Recommend(coor, user_rdd, 10)
    // val result1 = result.foreach(println)

    val writer = new PrintWriter(new File("d:/duowan.txt"))

    for (line <- result.collect()) {
      writer.println(line)
    }

  }


  def UserData(sc: SparkContext, input: String, split: String): (RDD[(String, String, Double)]) = {

    val user_rdd1 = sc.textFile(input, 100).filter(_.split(split).length == 3)

    val user_rdd2 = user_rdd1.map(line => {

      val fileds = line.split(split)

      (fileds(0), fileds(1), fileds(2).toDouble)

    })

    user_rdd2

  }


  def Cooccurrence(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备

    val user_rdd2 = user_rdd.map(f => (f._1, f._2)).sortByKey()

    user_rdd2.cache

    //  1 (用户：物品)笛卡尔积 (用户：物品) =>物品:物品组合

    val user_rdd3 = user_rdd2 join user_rdd2

    val user_rdd4 = user_rdd3.map(data => (data._2, 1))

    //  2 物品:物品:频次

    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    //  3 对角矩阵

    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)

    //  4 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算同现相似度（物品1，物品2，同现频次）

    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).

      join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,

      f._2._1._2, f._2._1._3, f._2._2)))

    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))

    //   6结果返回

    user_rdd12

  }

  def Cooccurrence_Euclidean(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备

    val user_rdd2 = user_rdd.map(f => (f._1, f._2)).sortByKey()

    user_rdd2.cache

    //  1 (用户：物品)笛卡尔积 (用户：物品) =>物品:物品组合

    val user_rdd3 = user_rdd2 join user_rdd2

    val user_rdd4 = user_rdd3.map(data => (data._2, 1))

    //  2 物品:物品:频次

    //val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)

    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    //  3 对角矩阵

    //val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)

    //  3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1）组合并累加   计算重叠数

    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)

    //  4 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8 = user_rdd7.join(user_rdd6)

    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))

    //   7 结果返回

    user_rdd9

  }

  def CosineSimilarity(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备

    val user_rdd2 = user_rdd.map(f => (f._1, (f._2, f._3))).sortByKey()

    user_rdd2.cache

    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合

    val user_rdd3 = user_rdd2 join user_rdd2

    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2）组合并累加

    val user_rdd5 = user_rdd4.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)

    //  3 对角矩阵

    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)

    //  4 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).

      join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,

      f._2._1._2, f._2._1._3, f._2._2)))

    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))

    //  7 结果返回

    user_rdd12

  }

  def Recommend(items_similar: RDD[(String, String, Double)], user_perf: RDD[(String, String, Double)], r_number: Int): (RDD[(String, String, Double)]) = {

    //  1 矩阵计算——i行与j列join

    val rdd_app1_R2 = items_similar.map(f => (f._2, (f._1, f._3))).join(user_perf.map(f => (f._2, (f._1, f._3))))

    //  2 矩阵计算——i行与j列元素相乘

    val rdd_app1_R3 = rdd_app1_R2.map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2))

    //  3 矩阵计算——用户：元素累加求和

    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x, y) => x + y).map(f => (f._1._1, (f._1._2, f._2)))

    //  4 矩阵计算——用户：用户对结果排序，过滤

    val rdd_app1_R5 = rdd_app1_R4.groupByKey()

    val rdd_app1_R6 = rdd_app1_R5.map(f => {

      val i2 = f._2.toBuffer

      val i2_2 = i2.sortBy(_._2)

      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))

      (f._1, i2_2.toIterable)

    })

    val rdd_app1_R7 = rdd_app1_R6.flatMap(f => {

      val id2 = f._2

      for (w <- id2) yield (f._1, w._1, w._2)

    })

    rdd_app1_R7

  }

  def EuclideanDistanceSimilarity(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备

    val user_rdd2 = user_rdd.map(f => (f._1, (f._2, f._3))).sortByKey()

    user_rdd2.cache

    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合

    val user_rdd3 = user_rdd2 join user_rdd2
    //f._2：合并之后的value，包含2个item的id和评分。
    //再_1选择第一个item，再_1选择id
    //再_1选择第一个item，再_2选择评分
    //((物品1的id,物品2的id)(物品1的评分,物品2的评分))
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1-评分2）组合并累加

    val user_rdd5 = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _)

    //  3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1）组合并累加   计算重叠数

    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)

    //  4 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8 = user_rdd7.join(user_rdd6)
    //(物品1, 物品2, 相似得分)
    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))

    //   7 结果返回

    user_rdd9

  }
  def EuclideanDistanceSimilarity_User(user_rdd: RDD[(String, String, Double)]): (RDD[(String, String, Double)]) = {

    //  0 数据做准备

    val user_rdd2 = user_rdd.map(f => (f._2, (f._1, f._3)))



    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合

    val user_rdd3 = user_rdd2 join user_rdd2
    //f._2：合并之后的value，包含2个item的id和评分。
    //再_1选择第一个item，再_1选择id
    //再_1选择第一个item，再_2选择评分
    //((物品1的id,物品2的id)(物品1的评分,物品2的评分))
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1-评分2）组合并累加

    val user_rdd5 = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _)

    //  3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1）组合并累加   计算重叠数

    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)

    //  4 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8 = user_rdd7.join(user_rdd6)
    //(物品1, 物品2, 相似得分)
    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))

    //   7 结果返回

    user_rdd9

  }


}



