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

  test("prototype") {
    val (columns, initDf) = read("/timeusage/atussum_100.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

//    initDf.show()

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

//    summaryDf.show()

    val groupedDf = timeUsageGroupedSql(summaryDf)

    groupedDf.show()
  }
}
