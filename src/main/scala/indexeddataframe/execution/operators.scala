package indexeddataframe.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EqualTo, Expression, Literal}
import indexeddataframe.{IRDD, InternalIndexedDF, Utils}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.slf4j.LoggerFactory

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
  private val logger = LoggerFactory.getLogger(classOf[IndexedOperatorExec])

  //override def outputPartitioning: Partitioning = child.outputPartitioning
  def executeIndexed(): IRDD

  // the number of the indexed column
  def indexColNo = 0

  /**
    * if the indexed operator is required to return rows (i.e.,
    * as for a regular spark DF operations) produce its rows by
    * scanning the index
    */
  override def doExecute() = {
    executeIndexed()
  }

  override def executeCollect(): Array[InternalRow] = {
    logger.debug("executing the collect operator")
    executeIndexed().collect()
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    logger.debug("executing the take operator")
    if (n == 0) {
      return new Array[InternalRow](0)
    }
    executeIndexed().take(n)
  }

  def executeGetRows(key: AnyVal): Array[InternalRow] = {
    val resultRDD = executeIndexed().get(key)
    resultRDD.collect()
  }

  def executeMultiGetRows(keys: Array[AnyVal]): Array[InternalRow] = {
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
  private val logger = LoggerFactory.getLogger(classOf[CreateIndexExec])

  override def output: Seq[Attribute] = child.output

  // we need to repartition when creating the Index in order to know how to partition the appends and join probes
  override def outputPartitioning = HashPartitioning(Seq(child.output(indexColNo)), sqlContext.getConf("spark.sql.shuffle.partitions").toInt)
  override def requiredChildDistribution: Seq[Distribution] = Seq(ClusteredDistribution(Seq(child.output(indexColNo))))

  override def executeIndexed(): IRDD = {
    logger.debug("executing the createIndex operator")

    // create the index
    val partitions = child.execute().mapPartitions[InternalIndexedDF](
      rowIter => Iterator(Utils.doIndexing(indexColNo, rowIter, output.map(_.dataType), output)),
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
  private val logger = LoggerFactory.getLogger(classOf[AppendRowsExec])

  override def output: Seq[Attribute] = left.output

  override def indexColNo = left.asInstanceOf[IndexedOperatorExec].indexColNo
  def distributionOutput = left.asInstanceOf[IndexedOperatorExec].indexColNo

  override def outputPartitioning = left.outputPartitioning
  override def requiredChildDistribution: Seq[Distribution] = Seq(ClusteredDistribution(Seq(output(distributionOutput))),
                                                              ClusteredDistribution(Seq(right.output(distributionOutput))))

  override def executeIndexed(): IRDD = {
    logger.debug("executing the appendRows operator")
    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val rightRDD = right.execute()

    val zippedResult = leftRDD.partitionsRDD.zipPartitions(rightRDD, true) { (leftIter, rightIter) =>
      val idf = leftIter.next().getSnapshot()
      while (rightIter.hasNext) {
        idf.appendRow(rightIter.next())
      }
      Iterator(idf)
    }
    Utils.ensureCached(new IRDD(leftRDD.colNo, zippedResult))
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
  private val logger = LoggerFactory.getLogger(classOf[IndexedBlockRDDScanExec])

  override def indexColNo = child.asInstanceOf[IndexedOperatorExec].indexColNo
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeIndexed(): IRDD = {
    logger.debug("executing the cache() operator")

    Utils.ensureCached(rdd)
  }
}

/**
  * operator that performs key lookups
  * @param key
  * @param child
  */
case class GetRowsExec(key: AnyVal, child: SparkPlan) extends UnaryExecNode {
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
case class IndexedFilterExec(condition: Expression, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def doExecute(): RDD[InternalRow] = {
    condition match {
      case EqualTo(_, literalValue: Literal) => {
        val key = literalValue.value.asInstanceOf[AnyVal]
        val rdd = child.asInstanceOf[IndexedOperatorExec].executeIndexed()
        val resultRDD = rdd.get(key)

        resultRDD
      }
      case _ => null
    }
  }
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
  private val logger = LoggerFactory.getLogger(classOf[IndexedShuffledEquiJoinExec])

  override def output: Seq[Attribute] = left.output ++ right.output

  // We're using this to force spark to shuffle the right relation in a similar way
  // to the left indexed relation such that we can correctly do the join
  override def requiredChildDistribution: Seq[Distribution] =  Seq(ClusteredDistribution(leftKeys), ClusteredDistribution(rightKeys))

  override def doExecute(): RDD[InternalRow] = {
    logger.debug("in the Shuffled JOIN operator")

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    left match {
      case _: IndexedOperatorExec => {
        val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
        val rightRDD = right.execute()

        val result = leftRDD.partitionsRDD.zipPartitions(rightRDD, true) { (leftIter, rightIter) =>
          // generate an unsafe row joiner
          leftSchema.add("prev", IntegerType)
          val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
          if (leftIter.hasNext) {
            val result = leftIter.next().multigetJoinedRight(rightIter, joiner, right.output, rightCol)
            result
          }
          else Iterator(null)
        }
        result
      }
      case _ => {
        val leftRDD = left.execute()
        val rightRDD = right.asInstanceOf[IndexedOperatorExec].executeIndexed()

        val result = rightRDD.partitionsRDD.zipPartitions(leftRDD, true) { (rightIter, leftIter) =>
          // generate an unsafe row joiner
          rightSchema.add("prev", IntegerType)
          val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
          if (rightIter.hasNext) {
            val result = rightIter.next().multigetJoinedLeft(leftIter, joiner, left.output, leftCol)
            result
          }
          else Iterator(null)
        }
        result
      }
    }
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
  private val logger = LoggerFactory.getLogger(classOf[IndexedBroadcastEquiJoinExec])

  override def output: Seq[Attribute] = left.output ++ right.output

  override def doExecute(): RDD[InternalRow] = {
    logger.debug("in the Broadcast JOIN operator")

    val leftSchema = StructType(left.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val rightSchema = StructType(right.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val leftRDD = left.asInstanceOf[IndexedOperatorExec].executeIndexed()
    val t1 = System.nanoTime()
    // broadcast the right relation
    val rightRDD = sparkContext.broadcast(right.executeCollect())
    val t2 = System.nanoTime()

    logger.debug("collect + broadcast time = %f".format( (t2-t1) / 1000000.0))

    val result = leftRDD.multigetBroadcast(rightRDD, leftSchema, rightSchema, right.output, rightCol)
    result
  }
}
