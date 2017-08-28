
package indexeddataframe

import org.apache.spark.sql._
import org.scalatest.FunSuite
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators


class IndexedDFTtest extends FunSuite {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark test app")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()

  import sparkSession.implicits._
  sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
  sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

  test("createIndex") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    val rows = idf.collect()

    assert(rows.length == df.collect().length)
  }

  test("getRows") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()

    val idf = df.createIndex(0)

    val rows = idf.getRows(1234)
    rows.show()

    assert(rows.collect().length == 2)
  }

  test("appendRows") {

    val df = Seq((1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()
    val df2 = Seq((1234, 7546, "a")).toDF("src", "dst", "tag")

    val idf = df.createIndex(0).cache()
    val idf2 = idf.appendRows(df2)

    val rows = idf2.getRows(1234)

    assert(rows.collect().length == 3)
  }

  test("join") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex(0).cache()

    myIDF.createOrReplaceTempView("indextable")
    df2.createOrReplaceTempView("nonindextable")

    myIDF.explain(true)
    df2.explain(true)

    val joinedDF = sparkSession.sql("select * from indextable join nonindextable on indextable.src = nonindextable.src")

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

  test("string index") {
    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef"), (1, -3, "abcde")).toDF("src", "dst", "tag")
    val df = Seq(("abcd")).toDF("tag")

    val myIDF = myDf.createIndex(2).cache()

    val result = myIDF.getRows("abcde".asInstanceOf[AnyVal])

    assert(result.collect().length == 2)
  }
}