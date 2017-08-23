package indexeddataframe

import indexeddataframe.InternalIndexedDF
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.storage.StorageLevel


object Utils {

  def defaultNoPartitions: Int = 16
  val defaultPartitioner: HashPartitioner = new HashPartitioner(defaultNoPartitions)

  /**
    * function that is executed when the index is created on a DataFrame
    * this function is called for each partition of the original DataFrame and it creates an [[InternalIndexedDF]]
    * that contains the rows of the partitions and a CTrie for storing an index
    * @param colNo
    * @param rows
    * @param types
    * @return
    */
  def doIndexing(colNo: Int, rows: Iterator[InternalRow], types: Seq[DataType], output: Seq[Attribute]): InternalIndexedDF = {
    val idf = new InternalIndexedDF
    idf.createIndex(types, output, colNo)
    idf.appendRows(rows)
    /*
    val iter = idf.get(32985348972561L)
    var nRows = 0
    while (iter.hasNext) {
      nRows += 1
      iter.next()
    }
    println("this item is repeated %d times on this partition".format(nRows))
    */
    idf
  }

  /**
    * function that ensures an RDD is cached
    * @param rdd
    * @param storageLevel
    * @tparam T
    * @return
    */
  def ensureCached[T](rdd: IRDD, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): IRDD = {
    //println(rdd.getStorageLevel + " --------- " + storageLevel)
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }

  def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }
}

/**
  * a custom RDD class that is composed of a number of partitions containing the rows of the
  * indexed dataframe; each of these partitions is represented as an [[InternalIndexedDF]]
  * @param colNo
  * @param partitionsRDD
  */
class IRDD(val colNo: Int, var partitionsRDD: RDD[InternalIndexedDF])
  extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[InternalIndexedDF].iterator(part, context).next.iterator
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD = partitionsRDD.persist(newLevel)
    this
  }

  /**
    * RDD function that returns an RDD of the rows containing the search key
    * @param key
    * @return
    */
  def get(key: AnyVal): RDD[InternalRow] = {
    //println("I have this many partitions: " + partitionsRDD.getNumPartitions)
    val res = partitionsRDD.flatMap { part =>
      part.get(key).map( row => row.copy() )
    }
    res
  }

  /** TODO: REDO this, it is not correct now!!! (many things have changed since its first implementation
    * RDD method that returns an RDD of rows containing the searched keys
    * @param keys
    * @return
    */
  def multiget(keys: Array[AnyVal]): RDD[InternalRow] = {
    //println("I have this many partitions: " + partitionsRDD.getNumPartitions)
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => part.next().multiget(keys), true)
    res

  }

  /**
    * multiget method used in the broadcast join
    * @param rightRDD
    * @param leftSchema
    * @param rightSchema
    * @param joinRightCol
    * @return
    */
  def multigetBroadcast(rightRDD: Broadcast[Array[InternalRow]],
                        leftSchema: StructType,
                        rightSchema: StructType,
                        rightOutput: Seq[Attribute],
                        joinRightCol: Int): RDD[InternalRow] = {
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => {
        val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
        val res = part.next().multigetJoined(rightRDD.value.toIterator, joiner, rightOutput, joinRightCol)
        res
      }, true)
    res
  }
}
