package indexeddataframe

import indexeddataframe.InternalIndexedDF
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeRowJoiner, UnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by alexuta on 18/07/17.
  */
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
  def doIndexing(colNo: Int, rows: Iterator[InternalRow], types: Seq[DataType]): InternalIndexedDF[Long] = {
    val idf = new InternalIndexedDF[Long]
    idf.createIndex(types, colNo)
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
}

/**
  * a custom RDD class that is composed of a number of partitions containing the rows of the
  * indexed dataframe; each of these partitions is represented as an [[InternalIndexedDF]]
  * @param colNo
  * @param partitionsRDD
  */
class IRDD(val colNo: Int, var partitionsRDD: RDD[InternalIndexedDF[Long]])
  extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[InternalIndexedDF[Long]].iterator(part, context).next.iterator
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
  def get(key: Long): RDD[InternalRow] = {
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
  def multiget(keys: Array[Long]): RDD[InternalRow] = {
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
                        joinRightCol: Int): RDD[InternalRow] = {
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => {
        val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
        val res = part.next().multigetBroadcast(rightRDD.value, joiner, joinRightCol)
        res
      }, true)
    res
  }

  /**
    * RDD method that performs a multiget by means of partitioning the input keys
    * and sending them to their appropriate partitions
    * should only probably be used in case the input array is very large,
    * otherwise multiget works pretty well
    * @param keys
    * @return
    */
  def multigetPartitioned(keys: Array[Long]): RDD[InternalRow] = {
    val kMap = keys.map( k => (k, 1))
    var kRDD = context.parallelize(kMap)
    kRDD = kRDD.partitionBy(Utils.defaultPartitioner)

    partitionsRDD.zipPartitions(kRDD, true) { (iterLeft, iterRight) =>
      val kBuf = new ArrayBuffer[Long]
      while (iterRight.hasNext) {
        kBuf.append(iterRight.next()._1)
      }
      iterLeft.next().multiget(kBuf.toArray)
    }
  }

  /**
    * helper function for appending rows in the InternalIndexedDF object
    * @param iter1 -> this is an iterator containing the original data
    * @param iter2 -> this is an iterator containing the rows to be inserted
    * @return -> returns an iterator containing the updated rows
    */
  def appendZipFunc(iter1: Iterator[InternalIndexedDF[Long]], iter2: Iterator[(Long, InternalRow)] ): Iterator[InternalIndexedDF[Long]] = {
    val idf = iter1.next()
    while (iter2.hasNext) {
      val value = iter2.next()._2
      idf.appendRow(value)
    }
    Iterator(idf)
  }

  /**
    * RDD function that appends a list of rows to the current data
    * the list of rows is partitioned here such that each row goes to the partition that is supposed to contain that key
    * @param rows
    * @return
    */
  def appendRows(rows: Seq[InternalRow]): IRDD = {
    val map = collection.mutable.Map[Long, InternalRow]()
    rows.foreach( row => {
      // !!! fix later to get the type properly, now we just assume the indexed column is of type long !!!
      val key = row.get(colNo, LongType).asInstanceOf[Long]
      map.put(key, row)
    })
    // make an rdd with the updates
    val updatesRDD = context.parallelize(map.toSeq)
    // partition it similarly to our current partitions
    val partitionedUpdates = updatesRDD.partitionBy(Utils.defaultPartitioner)
    // apply the updates
    val result = partitionsRDD.zipPartitions(partitionedUpdates, true)(appendZipFunc)
    // return a new RDD with the appended rows
    new IRDD(colNo, result)
  }
}
