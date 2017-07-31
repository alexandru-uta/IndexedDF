package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import indexeddataframe.{IRDD, InternalIndexedDF, Utils}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

trait IndexedOperatorExec extends SparkPlan {
  def executeBlocked(): IRDD

  /**
    * An Indexed operator cannot return rows, so this method should normally not be invoked.
    * Instead use executeBlocked, which returns the data as a collection of "indexed RDDs"
    *
    * However, when indexed data is cached, Spark SQL's InMemoryRelation attempts to call this
    * method and persist the resulting RDD. [[ConvertToIndexedOperators]] later eliminates the dummy
    * relation from the logical plan, but this only happens after InMemoryRelation has called this
    * method. We therefore have to silently return an empty RDD here.
    */
  override def doExecute() = {
    sqlContext.sparkContext.emptyRDD
    // throw new UnsupportedOperationException("use executeBlocked")
  }

  override def executeCollect(): Array[InternalRow] = {
      executeBlocked().collect()
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }
    executeBlocked().take(n)
  }
}

case class CreateIndexExec(colNo: Int, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): IRDD = {
    println("executing the createIndex operator")

    val partitions = child.execute().mapPartitions[InternalIndexedDF[Long]](
      rowIter => Iterator(Utils.doIndexing(colNo, rowIter.toSeq, output.map(_.dataType))),
      true)
    val ret = new IRDD(colNo, partitions)

    ret
  }
}

case class AppendRowsExec(rows: Seq[InternalRow], child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): IRDD = {
    println("executing the appendRows operator")

    val ret = child.asInstanceOf[IndexedOperatorExec].executeBlocked().appendRows(rows)

    ret
  }
}