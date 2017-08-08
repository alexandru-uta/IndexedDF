package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import java.util

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.CacheManager

import scala.collection.mutable.ArrayBuffer

object Test extends App {

  val nTimes = 1
  def getRows(idf: InternalIndexedDF[Long], myVal: Long): Int = {
    var count = 0
    val friendsIter = idf.get(myVal)
    if (friendsIter != null) {
      while (friendsIter.hasNext) {
        //println(res.next().copy().toString)
        val crntRow = friendsIter.next()
        count += 1
      }
    }
    count
  }

  val sparkSession = SparkSession.builder.
    master("local")//spark://alex-macbook.local:7077")
    .appName("spark test app")
    //.config("spark.logLineage", "true")
      .config("spark.driver.maxResultSize", "8g")
    .getOrCreate()

  import sparkSession.implicits._

  sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
  sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

  var df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/home/alex/projects/IndexedDF/pkp2.csv")

  var smallDF = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/home/alex/projects/IndexedDF/pers2.csv")

  df = df.repartition(4, $"src").cache()
  //smallDF = smallDF.cache()
  //smallDF.collect()
  smallDF.show(100)

  val idf2 = df.createIndex(0).cache()
  var size0 = idf2.collect().size

  /*
  println("============= Created Index =================")

  var appendList = Seq[InternalRow]()
  for (i <- 0 to 5) {
    val row = InternalRow.fromSeq(Seq(i.toLong, (i + 1).toLong, UTF8String.fromString("sdasda")))
    appendList = appendList :+ row
  }
  val testRow = InternalRow.fromSeq(Seq(2199023262994L, 21212L, UTF8String.fromString("sdasda")))
  appendList = appendList :+ testRow

  val idf3 = idf2.appendRows(appendList).cache()
  var size1 = idf3.collect().size

  //idf3.explain(true)

  println("============== Appended first batch of rows ================")

  val idf4 = idf3.appendRows(appendList).cache()
  var size2 = idf4.collect().size

  println("============== Appended second batch of rows ================")

  val idf5 = idf4.appendRows(appendList).cache()
  var size3 = idf5.collect().size

  println("============== Appended third batch of rows ================")

  println("init size = %d, append1 size = %d, append2 size = %d, append3 size = %d".format(size0, size1, size2, size3))

  */

  var t1 = System.nanoTime()
  val filteredRowsIDF = idf2.multigetRows(ArrayBuffer(32985348972561L, 2199023262994L, 10008L, 9998L, 1L, 9995L))
  var t2 = System.nanoTime()
  val szIDF = filteredRowsIDF.size

  df.createOrReplaceTempView("table1")
  val filteredDF = sparkSession.sql("select * from table1 where src = '32985348972561'")

  var t3 = System.nanoTime()
  val filteredRowsDF = filteredDF.collect()
  var t4 = System.nanoTime()
  val szDF = filteredRowsDF.size

  println("lookup on IDF took %f ms, DF took %f ms".format(((t2-t1) / 1000000.0), ((t4-t3) / 1000000.0)))
  println("lookup size of IDF = %d, on DF = %d".format(szIDF, szDF))


  idf2.createOrReplaceTempView("indexedtable")
  smallDF.createOrReplaceTempView("smalltable")

  val res = sparkSession.sql("SELECT * " +
                             "FROM indexedtable " +
                             "JOIN smalltable " +
                             "ON indexedtable.src = smalltable.id")
  res.explain(true)

  t3 = System.nanoTime()
  //res.collect()
  var size = 0
  val plan = res.queryExecution.executedPlan.execute()
  plan.foreachPartition( p => size += p.size )
  t4 = System.nanoTime()

  //res.show(100)

  println("join on IDF took %f ms, DF took %f ms".format(((t2-t1) / 1000000.0), ((t4-t3) / 1000000.0)))
  println(" join size on IDF = %d, on DF = %d".format(0, size))

  sparkSession.close()
  sparkSession.stop()
}
