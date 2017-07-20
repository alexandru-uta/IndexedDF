package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import indexeddataframe.{IRDD, InternalIndexedDF, Utils}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning


case class CreateIndexExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  def doExecute2(): IRDD[String] = {
    println("executing the createIndex operator")

    val partitions = child.execute().mapPartitions[InternalIndexedDF[String, InternalRow]](
      rowIter => Iterator(Utils.doIndexing(rowIter.toSeq, output.map(_.dataType))),
      true)
    val ret = new IRDD(partitions)

    println("number of partitions = " + ret.getNumPartitions)

    ret.foreachPartition( iter => println(iter))

    println(" ========== ")

    ret
  }

  override def doExecute(): RDD[InternalRow] = {
    val res = doExecute2()

    println(res.getNumPartitions)

   null
  }
}