package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.SparkSession
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators

object Example extends App {

  val nTimes = 2
  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark test app")
    .config("spark.driver.maxResultSize", "40g")
    // use the concurrent mark sweep GC as it achieves better performance than the others (according
    // to our experiments)
    .config("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC")
    .config("spark.sql.shuffle.partitions", "16")
    // increase the delay scheduling wait so as to achieve higher chances of locality
    .config("spark.locality.wait", "10")
    // use this, as otherwise, the join can be scheduled with locality for the "right" relation, which is not desirable
    // as we would have to move the indexed data, which is slow
    .config("spark.shuffle.reduceLocality.enabled", "false")
    .getOrCreate()

  import sparkSession.implicits._

  sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
  sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

  var df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/Users/alexuta/projects/IndexedDF/pkp3.csv")

  var smallDF = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/Users/alexuta/projects/IndexedDF/pers3.csv")

  smallDF = smallDF.cache()
  smallDF.collect()

  val idf2 = df.createIndex(0).cache()
  val plan = idf2.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeIndexed()
  plan.foreachPartition( p => println(p.size))


  idf2.createOrReplaceTempView("indexedtable")
  df.createOrReplaceTempView("table1")
  smallDF.createOrReplaceTempView("smalltable")

  var res = sparkSession.sql("SELECT * " +
                             "FROM indexedtable " +
                             "JOIN smalltable " +
                             "ON indexedtable.src = smalltable.id")

  var totalTime = 0.0
  for (i  <- 1 to nTimes) {
    val t1 = System.nanoTime()
    val plan = res.queryExecution.executedPlan.execute()
    plan.foreachPartition( p => println(p.size) )
    val t2 = System.nanoTime()
    println("join iteration %d took %f".format(i, (t2-t1)/1000000.0))
    totalTime += (t2 - t1)
  }

  val res2 = sparkSession.sql("SELECT * " +
    "FROM table1 " +
    "JOIN smalltable " +
    "ON table1.src = smalltable.id")


  println("join on IDF took %f ms".format((totalTime / (nTimes * 1000000.0))))


  sparkSession.close()
  sparkSession.stop()
}
