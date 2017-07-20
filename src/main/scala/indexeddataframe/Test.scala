package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import java.util

import org.apache.spark.sql.{Row, SparkSession}
import indexeddataframe.implicits._
import org.apache.spark.SparkEnv

object Test extends App {

  val nTimes = 10000
  def getFriends(idf: InternalIndexedDF[String, Row], myVal: String): Int = {
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

  var df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .load("/Users/alexuta/Desktop/test.csv")

  df.cache()
  df.show()

  df = df.repartition(4, $"src")

  val idf2 = df.createIndex(0)
  idf2.explain(true)

  idf2.collect()
  idf2.show()

  /*
  System.exit(0)

  val idf = new InternalIndexedDF
  idf.createIndex(df, 0)
  idf.appendRows(df.collect())

  // getting friends with the indexed DF
  val t1 = System.nanoTime()
  var i = 0
  var count = 0
  for (i <- 0 to nTimes) {
    count = getFriends(idf, "2199023262994")
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
