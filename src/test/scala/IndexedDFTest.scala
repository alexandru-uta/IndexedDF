
package indexeddataframe

import indexeddataframe.Test.sparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators


class IndexedDFTtest extends FunSuite {

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  test("createIndex") {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test app")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    val rows = idf.collect()

    assert(rows.length == df.collect().length)
  }

  test("getRows") {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test app")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    val rows = idf.getRows(1234)

    assert(rows.length == 2)
  }

  test("appendRows") {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test app")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()
    val df2 = Seq((1234, 7546, "a")).toDF("src", "dst", "tag")

    val idf = df.createIndex(0)
    val idf2 = idf.appendRows(df2)

    val rows = idf2.getRows(1234)

    assert(rows.length == 3)
  }

  test("join") {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test app")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()
    val df2 = Seq((1234)).toDF("src")

    val idf = df.createIndex(0)

    val joinedDF = idf.join(df2, Seq("src"))

    assert(joinedDF.collect().length == 2)
  }
}