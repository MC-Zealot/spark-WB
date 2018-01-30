package UserSimilarityByTags.MyTest

/**
 * Created by yizhou on 2018/01/11.
 */
object CosineSimilarity {
 /*
  * This method takes 2 equal length arrays of integers
    * It returns a double representing similarity of the 2 arrays
  * 0.9925 would be 99.25% similar
  * (x dot y)/||X|| ||Y||
  */
  def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }

  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct(x: Array[Int], y: Array[Int]): Int = {
    (for((a, b) <- x zip y) yield a * b) sum
  }

  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude(x: Array[Int]): Double = {
    math.sqrt(x map(i => i*i) sum)
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
}
