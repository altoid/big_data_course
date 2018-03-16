package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow.sc

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  ignore("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  ignore("simple") {
    import StackOverflow._
    val postings = List(
      Posting(1, 27233496, None, None, 0, Some("C#")),
      Posting(1, 23698767, None, None,9, Some("C#")),
      Posting(1, 5484340, None, None,0, Some("C#")),
      Posting(2, 5494879, None, Some(5484340),1, None),
      Posting(1, 9419744, None, None,2, Some("Objective-C")),
      Posting(1, 26875732, None, None,1, Some("C#")),
      Posting(1, 9002525, None, None,2, Some("C++")),
      Posting(2, 9003401, None, Some(9002525),4, None),
      Posting(2, 9003942, None, Some(9002525),1, None),
      Posting(2, 9005311, None, Some(9002525),0, None)
    )

    val rdd: RDD[Posting] = sc.parallelize(postings)
    rdd.collect().foreach(println)
  }

  test("scoredPostingsTest") {
    import StackOverflow._

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    val postings = List(
      Posting(1, 27233496, None, None, 0, Some("C#")),
      Posting(1, 23698767, None, None,9, Some("C#")),
      Posting(1, 5484340, None, None,0, Some("C#")),
      Posting(2, 5494879, None, Some(5484340),1, None),
      Posting(1, 9419744, None, None,2, Some("Objective-C")),
      Posting(1, 26875732, None, None,1, Some("C#")),
      Posting(1, 9002525, None, None,2, Some("C++")),
      Posting(2, 9003401, None, Some(9002525),4, None),
      Posting(2, 9003942, None, Some(9002525),1, None),
      Posting(2, 9005311, None, Some(9002525),0, None)
    )

    val rdd: RDD[Posting] = sc.parallelize(postings).cache()

    // there is no span() on rdds so we have to filter twice.
    // should be ok since we cached.

    val qa_rdd: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(rdd)
    qa_rdd.collect.foreach(println)

    val z: RDD[(Question, Iterable[Answer])] = qa_rdd.flatMap(_._2).groupByKey()

    println("=========================")
    z.collect.foreach(println)

//    (Posting(1,9002525,None,None,2,Some(C++)),CompactBuffer(Posting(2,9003401,None,Some(9002525),4,None), Posting(2,9003942,None,Some(9002525),1,None), Posting(2,9005311,None,Some(9002525),0,None)))
//    (Posting(1,5484340,None,None,0,Some(C#)),CompactBuffer(Posting(2,5494879,None,Some(5484340),1,None)))

    // want:  RDD[(Question, HighScore)]
    val w: RDD[(Question, HighScore)] = z.mapValues(i => answerHighScore(i.toArray))

    println("==================")
    w.collect.foreach(println)

    val scored = scoredPostings(qa_rdd)
    println("==================")
    scored.collect.foreach(println)
  }
}
