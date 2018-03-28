package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage._
import org.apache.spark.rdd.RDD

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("prototype") {
    val resource = "/timeusage/atussum_100.csv"
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]

//    println(headerColumns)

    val schema = dfSchema(headerColumns)

    val data: RDD[String] = rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    // data.collect.foreach(println)
    val (primary, working, other) = classifiedColumns(List("t01", "t01", "t08", "t02"))
    println(primary)
    println(working)
    println(other)
  }

}
