
package indexeddataframe

import org.apache.spark.sql._
import org.scalatest.FunSuite
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators


class IndexedDFTest extends FunSuite {

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

    assert(idf.collect().length == df.collect().length)
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
    val idf3 = idf2.appendRows(df2)

    //idf2.explain()
    //idf3.explain()

    val rows = idf.getRows(1234)
    val rows2 = idf2.getRows(1234)
    val rows3 = idf3.getRows(1234)

    val r3 = rows3.collect()
    val r2 = rows2.collect()
    val r1 = rows.collect()

    // check if the older dataframe does not see the update
    assert(r1.length == 2 && r2.length == 3 && r3.length == 4)
  }

  test("join") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex(0).cache()

    myIDF.createOrReplaceTempView("indextable")
    df2.createOrReplaceTempView("nonindextable")

    val joinedDF = sparkSession.sql("select * from indextable join nonindextable on indextable.src = nonindextable.src")

    joinedDF.explain(true)

    joinedDF.show()
    assert(joinedDF.collect().length == 2)
  }

  test("join2") {

    val myDf = Seq((1234, 12345, "abcd"), (1234, 102, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag")
    val df2 = Seq((1234, "test")).toDF("src", "data")

    val myIDF = myDf.createIndex(1).cache()
    //val myIDF = myDf.cache()

    myIDF.createOrReplaceTempView("indextable")
    df2.createOrReplaceTempView("nonindextable")

    // Join on non indexed column. Should fallback to normal non-indexed joins.
    val joinedDF = sparkSession.sql("select * from indextable join nonindextable on indextable.src = nonindextable.src")

    joinedDF.explain(true)

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

  test ("divergence") {

    val d1 = Seq((1230, 12345, "sdsad"), (1234, 12345, "abcd"), (1234, 12, "abcde"), (1237, 120, "abcdef") ).toDF("src", "dst", "tag").cache()
    val d2 = Seq((1235, 7546, "a")).toDF("src", "dst", "tag")
    val d3 = Seq((1236, 7546, "a")).toDF("src", "dst", "tag")

    val table1 = d1.createIndex(0).cache()
    val table2 = table1.appendRows(d2)
    val table3 = table1.appendRows(d3)

    //idf2.explain()
    //idf3.explain()

    //val rows = table1.getRows(1234)
    val rows2 = table2.getRows(1236)
    val rows3 = table3.getRows(1235)

    val r3 = rows3.collect()
    val r2 = rows2.collect()
    //val r1 = rows.collect()

    // check if the older dataframe does not see the update
    assert(r2.length == 0 && r3.length == 0)
  }
}