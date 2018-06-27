package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators

object BenchmarkPrograms {

  val nTimesRun = 5

  def triggerExecutionDF(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan.execute()
    plan.foreachPartition(p => println(p.size))
  }

  def triggerExecutionIndexedDF(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeIndexed()
    plan.foreachPartition(p => println(p.size))
  }

  def createIndexAndCache(df: DataFrame): DataFrame = {
    val indexed = df.createIndex(0).cache()
    triggerExecutionIndexedDF(indexed)
    indexed
  }

  def runJoin(indexedDF: DataFrame, nodesDF: DataFrame, sparkSession: SparkSession) = {
    indexedDF.createOrReplaceTempView("edges")
    nodesDF.createOrReplaceTempView("vertices")

    var res = sparkSession.sql("SELECT * " +
      "FROM edges " +
      "JOIN vertices " +
      "ON edges.src = vertices.id")

    // run the join a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("join iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("join on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def runScan(indexedDF: DataFrame, sparkSession: SparkSession) = {

    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges")

    // run the scan a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("scan iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("scan on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test app")
      .config("spark.driver.maxResultSize", "40g")
      // use the concurrent mark sweep GC as it achieves better performance than the others (according
      // to our experiments)
      .config("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC")
      .config("spark.sql.shuffle.partitions", "8")
      // increase the delay scheduling wait so as to achieve higher chances of locality
      .config("spark.locality.wait", "10")
      // use this, as otherwise, the join can be scheduled with locality for the "right" relation, which is not desirable
      // as we would have to move the indexed data, which is slow
      .config("spark.shuffle.reduceLocality.enabled", "false")
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    // load edges for a graph
    var edgesDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .load("/Users/alexuta/projects/IndexedDF/pkp3.csv")

    // load vertices for a graph
    var nodesDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .load("/Users/alexuta/projects/IndexedDF/pers3.csv")

    // cache the nodes and trigger the execution
    nodesDF = nodesDF.cache()
    triggerExecutionDF(nodesDF)

    // create the Index and cache the indexed DF
    val indexedDF = createIndexAndCache(edgesDF)

    // run a join between the edges of the graph and its nodes
    //runJoin(indexedDF, nodesDF, sparkSession)


    //run a scan of an indexed dataframe
    runScan(indexedDF, sparkSession)
    
    sparkSession.close()
    sparkSession.stop()
  }
}

