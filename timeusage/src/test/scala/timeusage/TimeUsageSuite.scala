package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage.{fsPath, spark}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("read file") {
    val resource = "/timeusage/atussum_100.csv"
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]

//    println(headerColumns)

    val stringColumn = headerColumns.head
    val doubleColumns = headerColumns.tail
    val fields = StructField(stringColumn, StringType, false) :: doubleColumns.map(c => StructField(c, DoubleType, false)).toList

    val result = StructType(fields)
    println(result)
  }

}
