package UserSimilarityByTags.MyTest

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
 * Created by yizhou on 2018/01/10.
 */
object CollaborativeFilteringSpark {
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  println(this.getClass().getSimpleName().filter(!_.equals('$')))
  //设置环境变量
  val sc = new SparkContext(conf)
  //实例化环境
  val users = sc.parallelize(Array("aaa", "bbb", "ccc", "ddd", "eee"))
  //设置用户
  val films = sc.parallelize(Array("smzdm", "ylxb", "znh", "nhsc", "fcwr")) //设置电影名

  val source = Map[String, Map[String, Int]]()
  //使用一个source嵌套map作为姓名电影名和分值的存储
  val filmSource = Map[String, Int]()

  //设置一个用以存放电影分的map
  def getSource(): Map[String, Map[String, Int]] = {
    //设置电影评分
    val user1FilmSource = Map("smzdm" -> 2, "ylxb" -> 3, "znh" -> 1, "nhsc" -> 0, "fcwr" -> 1)
    val user2FilmSource = Map("smzdm" -> 1, "ylxb" -> 2, "znh" -> 2, "nhsc" -> 1, "fcwr" -> 4)
    val user3FilmSource = Map("smzdm" -> 2, "ylxb" -> 1, "znh" -> 0, "nhsc" -> 1, "fcwr" -> 4)
    val user4FilmSource = Map("smzdm" -> 3, "ylxb" -> 2, "znh" -> 0, "nhsc" -> 5, "fcwr" -> 3)
    val user5FilmSource = Map("smzdm" -> 5, "ylxb" -> 3, "znh" -> 1, "nhsc" -> 1, "fcwr" -> 2)
    source += ("aaa" -> user1FilmSource) //对人名进行存储
    source += ("bbb" -> user2FilmSource) //对人名进行存储
    source += ("ccc" -> user3FilmSource) //对人名进行存储
    source += ("ddd" -> user4FilmSource) //对人名进行存储
    source += ("eee" -> user5FilmSource) //对人名进行存储
    source //返回嵌套map
  }

  //两两计算分值,采用余弦相似性
  def getCollaborateSource(user1: String, user2: String): Double = {
    val user1FilmSource = source.get(user1).get.values.toVector //获得第1个用户的评分
    val user2FilmSource = source.get(user2).get.values.toVector //获得第2个用户的评分
    val member = user1FilmSource.zip(user2FilmSource).map(d => d._1 * d._2).reduce(_ + _).toDouble //对公式分子部分进行计算
    val temp1 = math.sqrt(user1FilmSource.map(num => {
        //求出分母第1个变量值
        math.pow(num, 2) //数学计算
      }).reduce(_ + _)) //进行叠加
    val temp2 = math.sqrt(user2FilmSource.map(num => {
        ////求出分母第2个变量值
        math.pow(num, 2) //数学计算
      }).reduce(_ + _)) //进行叠加
    val denominator = temp1 * temp2 //求出分母
    member / denominator //进行计算
  }
  def similarity(t1: Map[String, Int], t2: Map[String, Int]): Double = {
    //word, t1 freq, t2 freq
    val m = scala.collection.mutable.HashMap[String, (Int, Int)]()

    val sum1 = t1.foldLeft(0d) {case (sum, (word, freq)) =>
      m += word ->(freq, 0)
      sum + freq
    }

    val sum2 = t2.foldLeft(0d) {case (sum, (word, freq)) =>
      m.get(word) match {
        case Some((freq1, _)) => m += word ->(freq1, freq)
        case None => m += word ->(0, freq)
      }
      sum + freq
    }

    val (p1, p2, p3) = m.foldLeft((0d, 0d, 0d)) {case ((s1, s2, s3), e) =>
      val fs = e._2
      val f1 = fs._1 / sum1
      val f2 = fs._2 / sum2
      (s1 + f1 * f2, s2 + f1 * f1, s3 + f2 * f2)
    }

    val cos = p1 / (Math.sqrt(p2) * Math.sqrt(p3))
    cos
  }

  def main(args: Array[String]) {
    getSource() //初始化分数
    var name = "bbb" //设定目标对象
    users.foreach(user => {
      //迭代进行计算
      println(name + " 相对于 " + user + "的相似性分数是：" + getCollaborateSource(name, user))
    })
//    println()
//    name = "aaa"
//    users.foreach(user => {
//      //迭代进行计算
//      println(name + " 相对于 " + user + "的相似性分数是：" + getCollaborateSource(name, user))
//    })

//    val user1FilmSource = Map("smzdm" -> 2, "ylxb" -> 3, "znh" -> 1, "nhsc" -> 0, "fcwr" -> 1)
//    val user2FilmSource = Map("smzdm" -> 1, "ylxb" -> 2, "znh" -> 2, "nhsc" -> 1, "fcwr" -> 4)
//    print(similarity(user1FilmSource,user2FilmSource))
    sc.textFile("file:///Users/Zealot/Desktop/exp/all").filter(_.split(",").length>29).map(_.split(",")).map{x=>
      val len = x.length
      (x(2),x(len-20).toDouble)}.reduceByKey(_+_).map(x=>x._1+"\t"+x._2).repartition(1).saveAsTextFile("file:///Users/Zealot/Desktop/uid_shouru4")


    sc.textFile("file:///Users/Zealot/Desktop/exp/all").filter(_.contains("2349616595")).filter(_.split(",").length>29).map(_.split(",")).map{x=>
       val len = x.length
       (x(2),x(len-20).toDouble)}.reduceByKey(_+_).collect
  }
}
