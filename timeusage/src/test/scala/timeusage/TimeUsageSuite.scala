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

  ignore("scribble") {
    val resource = "/timeusage/tiny.csv"
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]

    println(headerColumns)

    val schema = dfSchema(headerColumns) // returns a StructType

    println(schema)

    val data: RDD[String] = rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    // data.collect.foreach(println)
    val (primary, working, other) = classifiedColumns(headerColumns)
//    println(primary)
//    println(working)
//    println(other)

    val phonyData = List("first","1","234","666")
    println(row(phonyData))

    println(timeUsageGroupedSqlQuery("aeouoaeuae"))
  }

  test("grouped") {
//    +-----------+------+------+------------+----+-----+
//    |    working|   sex|   age|primaryNeeds|work|other|
//    +-----------+------+------+------------+----+-----+
//    |not working|female|active|        14.6| 1.0|  8.4|
//    |not working|female| young|        14.2| 0.0|  9.8|
//    |not working|  male|active|        10.9| 0.0| 13.1|
//    |not working|  male| young|        15.4| 0.0|  8.6|
//    |    working|female|active|        11.8| 3.6|  8.9|
//    |    working|female| elder|        13.6| 0.0| 10.2|
//    |    working|female| young|        10.2| 4.5|  9.7|
//    |    working|  male|active|        10.5| 6.4|  7.7|
//    |    working|  male| elder|        10.5| 3.7| 10.2|
//    |    working|  male| young|        14.3| 0.0|  9.7|
//    +-----------+------+------+------------+----+-----+

    val (columns, initDf) = read("/timeusage/atussum_100.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    val groupedDf = timeUsageGrouped(summaryDf)

    groupedDf.show()
  }

  test("groupedSql") {
//    +-----------+------+------+------------+----+-----+
//    |    working|   sex|   age|primaryNeeds|work|other|
//    +-----------+------+------+------------+----+-----+
//    |not working|female|active|        14.6| 1.0|  8.4|
//    |not working|female| young|        14.2| 0.0|  9.8|
//    |not working|  male|active|        10.9| 0.0| 13.1|
//    |not working|  male| young|        15.4| 0.0|  8.6|
//    |    working|female|active|        11.8| 3.6|  8.9|
//    |    working|female| elder|        13.6| 0.0| 10.2|
//    |    working|female| young|        10.2| 4.5|  9.7|
//    |    working|  male|active|        10.5| 6.4|  7.7|
//    |    working|  male| elder|        10.5| 3.7| 10.2|
//    |    working|  male| young|        14.3| 0.0|  9.7|
//    +-----------+------+------+------------+----+-----+
    val (columns, initDf) = read("/timeusage/atussum_100.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    //    initDf.show()

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    //    summaryDf.show()

    val groupedDf = timeUsageGroupedSql(summaryDf)

    groupedDf.show()
  }
}
