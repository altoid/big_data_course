package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow._

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

  ignore("scoredPostingsTest") {
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

    val scored: RDD[(Question, HighScore)] = scoredPostings(qa_rdd)
    println("==================")
    scored.collect.foreach(println)
  }

  ignore("vectorPostingsTest") {
    import StackOverflow._

    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    val scored: List[(Question, HighScore)] = List(
      (Posting(1, 6, None, None, 140, Some("CSS")), 67),
      (Posting(1, 42, None, None, 155, Some("PHP")), 89),
      (Posting(1, 72, None, None, 16, Some("Ruby")), 3),
      (Posting(1, 126, None, None, 33, Some("Java")), 30),
      (Posting(1, 174, None, None, 38, Some("C#")), 20)
    )

    val rdd: RDD[(Question, HighScore)] = sc.parallelize(scored)

    val vectors: RDD[(LangIndex, HighScore)] = vectorPostings(rdd)

    val result: List[(LangIndex, HighScore)] = vectors.collect.toList

    val test: List[(LangIndex, HighScore)] = List(
      (350000, 67),
      (100000, 89),
      (300000, 3),
      (50000, 30),
      (200000, 20)
    )

    assert(result === test)
  }

  test("langlabel") {
    val test: Iterable[(LangIndex, HighScore)] = List(
      (350000, 67),
      (350000, 67),
      (100000, 89),
      (350000, 67),
      (100000, 89),
      (300000, 3),
      (350000, 67),
      (100000, 89),
      (300000, 3),
      (50000, 30),
      (350000, 67),
      (100000, 89),
      (300000, 3),
      (50000, 30),
      (200000, 20)
    )

//    val x = test.map(t => (t._1 / langSpread, t._2)).groupBy(identity)
//    println(x)
//
//    val y = x.mapValues(_.size)
//    println(y)
//
//    val z = y.maxBy(_._2)
//    println(z)
//
//    val w = z._1._1
//    println(langs(w))

    val commonestLang = test.map(t => (t._1 / langSpread, t._2))
      .groupBy(identity)
      .mapValues(_.size)
      .maxBy(_._2)

    println(commonestLang)
    val commonestLangIndex = commonestLang._1._1  // this can't possibly be good style.

    println(commonestLangIndex)
    //      val langPercent: Double = (commonestLang._2 * 100.0) / vs.size // percent of the questions in the most common language

    val f = test.filter(_._1 == commonestLangIndex)
    println(f)
    val langPercent: Double = (test.filter(_._1 / langSpread == commonestLangIndex).size * 100.0) / test.size

    println(langPercent)

  }

  ignore("vector Postings test 2") {
    import StackOverflow._

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
  }
}
