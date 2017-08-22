package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import indexeddataframe.{IRDD, InternalIndexedDF, Utils}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.{StructField, StructType}


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
  //override def outputPartitioning: Partitioning = child.outputPartitioning
  def executeIndexed(): IRDD

  // the number of the indexed column
  def indexColNo = 0

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

  // TODO: this needs to be redone!!!!
  def executeMultiGetRows(keys: Array[Long]): Array[InternalRow] = {
    val resultRDD = executeIndexed().multiget(keys)
    resultRDD.collect()
  }
}

/**
  * physical operator that creates the index
  * @param indexColNo
  * @param child
  */
case class CreateIndexExec(override val indexColNo: Int, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning = HashPartitioning(Seq(child.output(indexColNo)), sqlContext.getConf("spark.sql.shuffle.partitions").toInt)
  override def requiredChildDistribution: Seq[Distribution] = Seq(ClusteredDistribution(Seq(child.output(indexColNo))))

  override def executeIndexed(): IRDD = {
    println("executing the createIndex operator")

    // we need to repartition when creating the Index in order to know how to partition the appends and join probes
    // and for the repartitioning we need an RDD[(key, row)] instead of RDD[row]
    //val pairLongRow = child.execute().map ( row => (row.get(colNo, LongType).asInstanceOf[Long], row.copy()) )
    // do the repartitioning
    //val repartitionedPair = pairLongRow.partitionBy(Utils.defaultPartitioner)
    // create the index
    val partitions = child.execute().mapPartitions[InternalIndexedDF[Long]](
      rowIter => Iterator(Utils.doIndexing(indexColNo, rowIter, output.map(_.dataType))),
      true)
    val ret = new IRDD(indexColNo, partitions)
    Utils.ensureCached(ret)
  }
}

/**
  * physical operator that appends rows to an indexed DataFrame
  * @param left the indexed DataFrame
  * @param right a regular DataFrame
  */
case class AppendRowsExec(left: SparkPlan, right: SparkPlan) extends BinaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = left.output

  override def indexColNo = left.asInstanceOf[IndexedOperatorExec].indexColNo
  def distributionOutput = left.asInstanceOf[IndexedOperatorExec].indexColNo

  override def outputPartitioning = left.outputPartitioning
  override def requiredChildDistribution: Seq[Distribution] = Seq(ClusteredDistribution(Seq(output(distributionOutput))),
                                                              ClusteredDistribution(Seq(right.output(distributionOutput))))

  override def executeIndexed(): IRDD = {
    println("executing the appendRows operator")
    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val rightRDD = right.execute()

    val result = leftRDD.partitionsRDD.zipPartitions(rightRDD, true) { (leftIter, rightIter) =>
      val idf = leftIter.next()
      while (rightIter.hasNext) {
        idf.appendRow(rightIter.next())
      }
      Iterator(idf)
    }
    Utils.ensureCached(new IRDD(leftRDD.colNo, result))
  }
}

/**
  * a physical operator that is used to replace the InMemoryRelation of default Spark,
  * as InMemoryRelation stores CachedBatches, while in our indexed DataFrame we do not want to change
  * the representation, we just "cache" the data structure as is
  * @param output
  * @param rdd
  * @param child
  */
case class IndexedBlockRDDScanExec(output: Seq[Attribute], rdd: IRDD, child: SparkPlan)
  extends LeafExecNode with IndexedOperatorExec {

  override def indexColNo = child.asInstanceOf[IndexedOperatorExec].indexColNo
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeIndexed(): IRDD = {
    println("executing the cache() operator")

    Utils.ensureCached(rdd)
  }
}

/**
  * operator that performs key lookups
  * @param key
  * @param child
  */
case class GetRowsExec(key: Long, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val resultRDD = rdd.get(key)

    resultRDD
  }
}

/**
  * dummy filter object, does not do anything atm
  * will be used in the future for applying filter on the indexed DataFrame
  * @param condition
  * @param child
  */
case class IndexedFilterExec(condition: Expression, child: SparkPlan) extends UnaryExecNode with IndexedOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def indexColNo = child.asInstanceOf[IndexedOperatorExec].indexColNo
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def executeIndexed(): IRDD = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
}

/**
  * physical operator to performed a shuffled equijoin between our indexed DataFrame and a regular DataFrame
  * @param left the indexed DataFrame
  * @param right a regular dataFrame
  * @param leftCol the number of the column on which the join is performed for the left table
  * @param rightCol the number of the column on which the join is performed for the left table
  * @param leftKeys the left join keys
  * @param rightKeys the right join keys
  */
case class IndexedShuffledEquiJoinExec(left: SparkPlan, right: SparkPlan, leftCol: Int, rightCol: Int,
                                       leftKeys: Seq[Expression], rightKeys: Seq[Expression]) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  // We're using this to force spark to shuffle the right relation in a similar way
  // to the left indexed relation such that we can correctly do the join
  override def requiredChildDistribution: Seq[Distribution] =  Seq(ClusteredDistribution(leftKeys), ClusteredDistribution(rightKeys))
    //Seq(UnknownPartitioning(Utils.defaultNoPartitions), HashPartitioning(rightKeys, Utils.defaultNoPartitions))

  override def doExecute(): RDD[InternalRow] = {
    println("in the Shuffled JOIN operator")

    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val rightRDD = right.execute()

    // we need to create an RDD[(key, row)] instead of RDD[row] in order to be able to repartition
    // otherwise we wouldn't be able to know where to send each of these partitions
    //var pairRDD = rightRDD.map( row => (row.get(rightCol, LongType).asInstanceOf[Long], row.copy()))
    // repartition in the same way as the Indexed Data Frame
    //pairRDD = pairRDD.partitionBy(Utils.defaultPartitioner)

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val result = leftRDD.partitionsRDD.zipPartitions(rightRDD, true) { (leftIter, rightIter) =>
      // generate an unsafe row joiner
      val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
      if (leftIter.hasNext) {
        val result = leftIter.next().multigetJoined(rightIter, joiner, rightCol)
        result
      }
      else Iterator(null)
    }
    result
  }

}

/**
  * physical operator that performs a broadcast equi join between an indexed DataFrame and a regular DataFrame
  * @param left the indexed DataFrame
  * @param right a regular DataFrame
  * @param leftCol
  * @param rightCol
  */
case class IndexedBroadcastEquiJoinExec(left: SparkPlan, right: SparkPlan, leftCol: Int, rightCol: Int) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def doExecute(): RDD[InternalRow] = {
    println("in the Broadcast JOIN operator")

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val t1 = System.nanoTime()
    // broadcast the right relation
    val rightRDD = sparkContext.broadcast(right.executeCollect())
    val t2 = System.nanoTime()

    println("collect + broadcast time = %f".format( (t2-t1) / 1000000.0))

    val result = leftRDD.multigetBroadcast(rightRDD, leftSchema, rightSchema, rightCol)
    result
  }
}
