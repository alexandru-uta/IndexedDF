package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, UnsafeProjection}
import indexeddataframe.{IRDD, InternalIndexedDF, Utils}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.types.LongType

import scala.collection.mutable.ArrayBuffer

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
  def executeIndexed(): IRDD

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
      println("executing the collect operator")
      executeIndexed().collect()
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    println("executing the take operator")
    if (n == 0) {
      return new Array[InternalRow](0)
    }
    executeIndexed().take(n)
  }

  def executeGetRows(key: Long): Array[InternalRow] = {
    val resultRDD = executeIndexed().get(key)
    resultRDD.collect()
  }

  def executeMultiGetRows(keys: ArrayBuffer[Long]): Array[InternalRow] = {
    val resultRDD = executeIndexed().multiget(keys)
    resultRDD.collect()
  }
}

case class CreateIndexExec(colNo: Int, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def executeIndexed(): IRDD = {
    println("executing the createIndex operator")
    val partitions = child.execute().mapPartitions[InternalIndexedDF[Long]](
      rowIter => Iterator(Utils.doIndexing(colNo, rowIter.toSeq, output.map(_.dataType))),
      true)
    val ret = new IRDD(colNo, partitions)
    Utils.ensureCached(ret)
  }
}

case class AppendRowsExec(rows: Seq[InternalRow], child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def executeIndexed(): IRDD = {
    println("executing the appendRows operator")
    val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed().appendRows(rows)
    Utils.ensureCached(rdd)
  }
}

case class IndexedBlockRDDScanExec(output: Seq[Attribute], rdd: IRDD)
  extends LeafExecNode with IndexedOperatorExec {

  override def executeIndexed(): IRDD = {
    println("executing the cache() operator")

    Utils.ensureCached(rdd)
  }
}

case class GetRowsExec(key: Long, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val resultRDD = rdd.get(key)

    resultRDD
  }
}

case class IndexedFilterExec(condition: Expression, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output
  override def executeIndexed(): IRDD = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
}

case class IndexedEquiJoinExec(left: SparkPlan, right: SparkPlan, leftCol: Int, rightCol: Int) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output

  override def doExecute(): RDD[InternalRow] = {
    println("in the JOIN operator")

    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val rightRDD = right.execute()

    // create a projection that contains only the column of interest for the join
    //val rightProj = rightRows.mapPartitions( part => {
    //  val proj = UnsafeProjection.create(Seq(right.output(rightCol)), right.output)
    //  part.map( row => proj(row) )
    //})

    val rows = rightRDD.collect()
    val buffer = new ArrayBuffer[Long]()
    var i = 0
    rows.foreach( row => {
      println(row.copy().toString)
      val key = row.copy().get(rightCol, LongType).asInstanceOf[Long]
      println(key + " - " + i)
      buffer.append(key)
      i += 1
    } )

    //println(buffer.size)

    //val result = leftRDD.multiget(buffer)

    //result.collect().foreach( row => println(row.toString) )

    //result

    null
  }

}