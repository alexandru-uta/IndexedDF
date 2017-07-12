/**
  * Created by alexuta on 06/07/17.
  */

import java.util

import Test.idf
import org.apache.spark.sql.execution.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.memory.MemoryMode
import indexeddataframe.InternalIndexedDF

object Test extends App {

  def getFriends(idf: InternalIndexedDF, myVal: String): util.ArrayList[String] = {
    // compute friends of friends on the IDF
    val friendsIDs = new util.ArrayList[String]
    val friendsIter = idf.get(myVal)
    if (friendsIter != null) {
      while (friendsIter.hasNext) {
        //println(res.next().copy().toString)
        val crntRow = friendsIter.next()
        friendsIDs.add(crntRow.getString(1))
      }
    }
    friendsIDs
  }

  val sparkSession = SparkSession.builder.
    master("local")//spark://alex-macbook.local:7077")
    .appName("spark test app")
    .getOrCreate()

  val df = sparkSession.read
    .format("com.databricks.spark.csv")
    .option("header", "true") //reading the headers
    .option("delimiter", "|")
    .load("/Users/alexuta/Desktop/pkp.csv")

  df.cache()
  df.show()

  val idf = new InternalIndexedDF
  idf.createIndex(df, 0)
  idf.appendRows(df.collect())

  // getting friends with the indexed DF
  val t1 = System.nanoTime()
  var i = 0
  for (i <- 0 to 100000) {
    val friends = getFriends(idf, "2199023262994")
  }
  val t2 = System.nanoTime()

  //println("we have %d tuples".format(friends.size()))

  df.createOrReplaceTempView("table1")

  // getting friends in spark sql
  val df2 = sparkSession.sql("select * from table1 where src = '2199023262994'")
  //println(df.explain)//.collect()
  val cachedPlan = df2.queryExecution.executedPlan.execute()
  val t3 = System.currentTimeMillis()
  cachedPlan.foreachPartition(i => println(i.length))
  val t4 = System.currentTimeMillis()

  // getting friends of friends with the indexed DF
  var friendIndex:Int = 0
  val friends2 = getFriends(idf, "2199023262994")
  val friendsOfFriends = new util.HashMap[String,Integer]
  for (friendIndex <- 0 to friends2.size()-1) {
    val crntFriends = getFriends(idf, friends2.get(friendIndex))
    var i = 0
    for (i <- 0 to crntFriends.size() -1) {
      friendsOfFriends.put(crntFriends.get(i),1)
    }
  }

  val t5 = System.currentTimeMillis()

  // getting friends of friends using spark sql
  val fof = sparkSession.sql("select * from table1 where src in (select dst from table1 where src='2199023262994') ")
  //println(df.explain)//.collect()
  fof.queryExecution.executedPlan.execute().foreachPartition(i => println(i.length))

  val t6 = System.currentTimeMillis()

  //println(fof.count)
  //println(friendsOfFriends.size())

  println("friends on IDF took %f ms, DF took %d ms".format((t2-t1) / 1000000.0 , t4-t3))
  println("friends of friends on IDF took %d ms, on DF took %d ms".format(t5-t4, t6-t5))

  sparkSession.close()
  sparkSession.stop()
}
