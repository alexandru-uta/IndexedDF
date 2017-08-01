package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import java.util

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.{Row, SparkSession}
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

object Test extends App {

  val nTimes = 1
  def getFriends(idf: InternalIndexedDF[Long], myVal: Long): Int = {
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
    .getOrCreate()

  import sparkSession.implicits._

  sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
  sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)
  var df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", true)
    .load("/home/alex/projects/test3.csv")

  df.cache()
  df.collect()

  df = df.repartition(4, $"src")

  val idf2 = df.createIndex(0).cache()
  val size0 = idf2.collect().size

  println("============= Created Index =================")

  var appendList = Seq[InternalRow]()
  for (i <- 0 to 5) {
    val row = InternalRow.fromSeq(Seq(i.toLong, (i + 1).toLong, UTF8String.fromString("sdasda")))
    appendList = appendList :+ row
  }
  val testRow = InternalRow.fromSeq(Seq(2199023262994L, 21212L, UTF8String.fromString("sdasda")))
  appendList = appendList :+ testRow
  println(appendList)

  val idf3 = idf2.appendRows(appendList).cache()
  val size1 = idf3.collect().size

  println("============== Appended first batch of rows ================")

  val idf4 = idf3.appendRows(appendList)
  var size2 = 0
  val plan = idf4.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeBlocked()
  plan.foreachPartition( it => { size2 += it.size })

  println(" ====================== Appended second batch of rows ==============")

  println("init size = %d, append1 size = %d, append2 size = %d".format(size0, size1, size2))
  idf4.explain(true)

  /*
  //System.exit(0)

  val idf = new InternalIndexedDF[Long]
  idf.createIndex(df, 0)
  idf.appendRows(df.collect())

  // getting friends with the indexed DF
  val t1 = System.nanoTime()
  var i = 0
  var count = 0
  for (i <- 0 to nTimes) {
    count = getFriends(idf, 2199023262994L)
  }
  val t2 = System.nanoTime()

  println("we have %d tuples".format(count))

  df.createOrReplaceTempView("table1")

  // getting friends in spark sql
  val df2 = sparkSession.sql("select * from table1 where src = '2199023262994'")
  //println(df.explain)//.collect()
  val cachedPlan = df2.queryExecution.executedPlan.execute()
  val t3 = System.nanoTime()
  cachedPlan.foreachPartition(i => println(i.length))
  val t4 = System.nanoTime()

  println("lookup on IDF took %f ms, DF took %f ms".format(((t2-t1) / 1000000.0) / nTimes , (t4-t3) / 1000000.0))
  */

  sparkSession.close()
  sparkSession.stop()
}
