package indexeddataframe

/**
  * Created by alexuta on 06/07/17.
  */

import indexeddataframe.execution.IndexedOperatorExec
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import indexeddataframe.implicits._
import indexeddataframe.logical.ConvertToIndexedOperators
import org.apache.spark.sql.types._

object BenchmarkPrograms {

  val nTimesRun = 200

  def triggerExecutionDF(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan.execute()
    plan.foreachPartition(p => println("partition size = " + p.length))
  }

  def triggerExecutionDF2(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan.execute()
    plan.foreachPartition(p => println("final part size = " + p.length))
  }

  def triggerExecutionIndexedDF(df: DataFrame) = {
    val plan = df.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeIndexed()
    plan.foreachPartition(p => println("indexed part size = " + p.length))
  }

  def createIndexAndCache(df: DataFrame): DataFrame = {
    val t1 = System.nanoTime()
    val indexed = df.createIndex(0).cache()
    triggerExecutionIndexedDF(indexed)
    val t2 = System.nanoTime()
    println("Index Creation took %f ms".format((t2 - t1) / 1000000.0))
    indexed
  }

  def runJoin(indexedDF: DataFrame, nodesDF: DataFrame, sparkSession: SparkSession) = {
    indexedDF.createOrReplaceTempView("edges")
    nodesDF.createOrReplaceTempView("vertices")

    var res = sparkSession.sql("SELECT * " +
      "FROM edges " +
      "JOIN vertices " +
      "ON edges.src = vertices.id")

    res.explain(true)

    // run the join a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF2(res)
      //res.count()

      val t2 = System.nanoTime()
      println("join iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }
    //println("resulting DF size = " + res.count())
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

  def runFilter(indexedDF: DataFrame, sparkSession: SparkSession) = {

    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges where edges.src > 100000000")
    res.explain(true)

    // run the scan a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("filter iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("filter on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def runEqFilter(indexedDF: DataFrame, sparkSession: SparkSession) = {

    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT * FROM edges where edges.src = 100000000")
    res.explain(true)

    // run the scan a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("filter iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("equality filter on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def runAgg(indexedDF: DataFrame, sparkSession: SparkSession) = {

    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT sum(src) FROM edges")

    // run the scan a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("agg iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("agg on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def runProj(indexedDF: DataFrame, sparkSession: SparkSession) = {

    indexedDF.createOrReplaceTempView("edges")

    val res = sparkSession.sql("SELECT dst FROM edges")

    // run the scan a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      triggerExecutionDF(res)

      val t2 = System.nanoTime()
      println("proj iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }

    println("proj on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def runAppend(indexedDF: DataFrame, appendDF:DataFrame, sparkSession: SparkSession) = {
    var indexedDFRes = indexedDF.appendRows(appendDF).cache()
    // run the operation a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      indexedDFRes = indexedDFRes.appendRows(appendDF)
      //triggerExecutionDF(indexedDFRes)

      val t2 = System.nanoTime()
      println("append iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }
    val t1 = System.nanoTime()
    val nRows = indexedDFRes.count()
    val t2 = System.nanoTime()
    println("resulting DF size = " + nRows)
    //totalTime = t2 - t1
    println("append on Indexed DataFrame took %f ms".format((totalTime / ((1) * 1000000.0))))
    //indexedDFRes.explain(true)
  }

  def runJoinAppend(indexedDF: DataFrame, nodesDF: DataFrame, appendDF:DataFrame, sparkSession: SparkSession) = {

    var indexedDFRes = indexedDF.appendRows(appendDF).cache()
    var res:DataFrame = null
    // run the join a few times and compute the average
    var totalTime = 0.0
    for (i <- 1 to nTimesRun) {
      val t1 = System.nanoTime()

      indexedDFRes.createOrReplaceTempView("edges")
      nodesDF.createOrReplaceTempView("vertices")

      res = sparkSession.sql("SELECT * " +
        "FROM edges " +
        "JOIN vertices " +
        "ON edges.src = vertices.id")
      triggerExecutionDF(res)

      // once in every 5 experiments, append some rows
      if (i % 5 == 0) indexedDFRes = indexedDFRes.appendRows(appendDF).cache()

      val t2 = System.nanoTime()
      println("join iteration %d took %f".format(i, (t2 - t1) / 1000000.0))

      if (i > 1) totalTime += (t2 - t1)
    }
    println("resulting DF size = " + res.count())
    println("joinAppend on Indexed DataFrame took %f ms".format((totalTime / ((nTimesRun - 1) * 1000000.0))))
  }

  def getSizeInBytes(df: DataFrame) = {
    //df.queryExecution.optimizedPlan.statistics.sizeInBytes
  }

  def main(args: Array[String]): Unit = {

    var delimiter1 = ""
    var delimiter2 = ""
    var path1 = ""
    var path2 = ""
    var partitions = ""
    var master = ""


    if (args.length < 6) {
      println("your args were: ")
      args.foreach( arg => print(arg + " "))
      println()
      println("not enough arguments!")
      println("please provide the delimiters of the csv files, the paths, and the number of partitions")
      System.exit(0)
    } else {
      delimiter1 = args(0)
      path1 = args(1)
      delimiter2 = args(2)
      path2 = args(3)
      partitions = args(4)
      master = args(5)
    }

    val sparkSession = SparkSession.builder.
      master(master)
      .appName("spark test app")
      .config("spark.driver.maxResultSize", "40g")
      // use the concurrent mark sweep GC as it achieves better performance than the others (according
      // to our experiments)
      .config("spark.executor.extraJavaOptions", "-javaagent:/home/aua400/updatableGraphs/IndexedDF/lib/jamm-0.3.4-SNAPSHOT.jar -XX:+UseConcMarkSweepGC -XX:+UseParNewGC")
      .config("spark.sql.shuffle.partitions", partitions)
      // increase the delay scheduling wait so as to achieve higher chances of locality
      .config("spark.locality.wait", "200")
      // use this, as otherwise, the join can be scheduled with locality for the "right" relation, which is not desirable
      // as we would have to move the indexed data, which is slow
      .config("spark.shuffle.reduceLocality.enabled", "false")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .getOrCreate()

    sparkSession.sparkContext.addJar("/home/aua400/updatableGraphs/IndexedDF/lib/jamm-0.3.4-SNAPSHOT.jar")

    import sparkSession.implicits._

    sparkSession.experimental.extraStrategies = (Seq(IndexedOperators) ++ sparkSession.experimental.extraStrategies)
    sparkSession.experimental.extraOptimizations = (Seq(ConvertToIndexedOperators) ++ sparkSession.experimental.extraOptimizations)

    val edgeSchema = StructType(Array(
      StructField("src", LongType, false),
      StructField("dst", LongType, false),
      StructField("cost", FloatType, true)))
    val nodeSchema = StructType(Array(
      StructField("id", LongType, false),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("gender", StringType, true),
      StructField("birthday", DateType, true),
      StructField("creationDate", StringType, true),
      StructField("locationIP", StringType, true),
      StructField("browserUsed", StringType, true)))

    // load edges for a graph
    var edgesDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter1)
      .schema(edgeSchema)
      .load(path1)
    
    // load edges for appending
    var appendDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter1)
      .schema(edgeSchema)
      .load("/var/scratch/aua400/datagen-edges-1M.csv")

    // load vertices for a graph
    var nodesDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter2)
      .schema(nodeSchema)
      .load(path2)

    // cache the nodes and trigger the execution
    nodesDF = nodesDF.cache()
    edgesDF = edgesDF.cache()
    //appendDF = appendDF.cache()
    triggerExecutionDF(nodesDF)
    triggerExecutionDF(edgesDF)
    //triggerExecutionDF(appendDF)

    //println("cached non-indexed = " + getSizeInBytes(edgesDF) / (1024 * 1024))
    //return 

    // create the Index and cache the indexed DF
    val indexedDF = createIndexAndCache(edgesDF)
    //val indexedDF = edgesDF

    // run a join between the edges of the graph and its nodes
    runJoin(indexedDF, nodesDF, sparkSession)

    // run append
    //runAppend(indexedDF, appendDF, sparkSession)

    indexedDF.show(10)


    return

    //run a scan of an indexed dataframe
    runScan(indexedDF, sparkSession)

    // run a filter on an indexed dataframe
    runFilter(indexedDF, sparkSession)

    // run an equality filter on an indexed dataframe
    runEqFilter(indexedDF, sparkSession)
    
    // run an aggregate on an indexed dataframe
    runAgg(indexedDF, sparkSession)

    // run a projection on an indexed dataframe
    runProj(indexedDF, sparkSession)

    sparkSession.close()
    sparkSession.stop()
  }
}


